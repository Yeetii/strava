using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Shared.Services.Shards;

namespace Backend;

public class VisitedPathsWorker(
    ILogger<VisitedPathsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<VisitedPath> _visitedPathsCollection,
    ShardFeatureClient _highwaysShardFeatureClient,
    ServiceBusClient serviceBusClient,
    ServiceBusAdministrationClient serviceBusAdministrationClient,
    UserSyncStatusService _userSyncStatusService)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;

    // 50m ≈ 0.00045° – use a slightly larger threshold for a rough but fast check
    private const double ProximityThresholdDegrees = 0.0005;

    // Grid cell size matches the proximity threshold for O(1) lookups
    private const double GridCellSize = ProximityThresholdDegrees;

    internal sealed record PathSectionCells(
        IReadOnlyList<(int LatCell, int LngCell)> First,
        IReadOnlyList<(int LatCell, int LngCell)> Middle,
        IReadOnlyList<(int LatCell, int LngCell)> Last);

    internal sealed record PathGridIndex(
        IReadOnlyList<Feature> Paths,
        Dictionary<(int LatCell, int LngCell), List<int>> CellToPathIndices,
        IReadOnlyList<PathSectionCells?> SectionCellsByPathIndex);

    private sealed class ActivityLinkedDocumentProjection
    {
        public required string Id { get; init; }
        public List<string>? ActivityIds { get; init; }
    }

    private record ActivitySlim(string Id, string UserId, string? Polyline, string? SummaryPolyline);

    [Function(nameof(VisitedPathsWorker))]
    public async Task Run(
        [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.CalculateVisitedPathsJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] jobs,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        if (await ServiceBusRescheduler.TryDeferForBackpressureAsync(
                serviceBusAdministrationClient,
                _serviceBusClient,
                Shared.Constants.ServiceBusConfig.CalculateVisitedPathsJobs,
                jobs,
                actions,
                _logger,
                cancellationToken))
        {
            return;
        }

        var ids = jobs.Select(x => x.Body.ToString()).ToList();
        var activities = await FetchActivitySlims(ids);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(a.Polyline ?? a.SummaryPolyline))
            .ToList();
        var activitiesById = activitiesList.ToDictionary(a => a.Id);

        // Decode polylines once and reuse
        var decodedActivities = activitiesList.ToDictionary(
            a => a.Id,
            a => GeoSpatialFunctions.DecodePolyline(a.Polyline ?? a.SummaryPolyline).ToList());

        // Renew locks before the potentially slow shared data fetch
        var realJobs = jobs.Where(ServiceBusRescheduler.HasRealLockToken).ToList();
        await Task.WhenAll(realJobs.Select(async j =>
        {
            try { await actions.RenewMessageLockAsync(j); }
            catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                _logger.LogWarning("Lock lost before renewal for message {MessageId}; it will be redelivered.", j.MessageId);
            }
        }));

        var nearbyPaths = (await FetchNearbyPaths(decodedActivities)).ToList();
        var pathGridIndex = BuildPathGridIndex(nearbyPaths);

        // Renew locks again after the potentially slow shared data fetch
        await Task.WhenAll(realJobs.Select(async j =>
        {
            try { await actions.RenewMessageLockAsync(j); }
            catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                _logger.LogWarning("Lock lost after fetch renewal for message {MessageId}; it will be redelivered.", j.MessageId);
            }
        }));

        foreach (var job in jobs)
            await ProcessJob(job, actions, activitiesById, decodedActivities, pathGridIndex, cancellationToken);
    }

    private async Task ProcessJob(
        ServiceBusReceivedMessage job,
        ServiceBusMessageActions actions,
        Dictionary<string, ActivitySlim> activitiesById,
        Dictionary<string, List<Coordinate>> decodedActivities,
        PathGridIndex pathGridIndex,
        CancellationToken cancellationToken)
    {
        var activityId = job.Body.ToString();
        try
        {
            activitiesById.TryGetValue(activityId, out var activity);

            if (activity == null || !decodedActivities.TryGetValue(activityId, out var activityPoints) || activityPoints.Count == 0)
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activityId);
                if (activity != null)
                {
                    await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedPaths, cancellationToken);
                }
                if (ServiceBusRescheduler.HasRealLockToken(job)) await actions.CompleteMessageAsync(job);
                return;
            }

            var activityGrid = BuildSpatialGrid(activityPoints);
            var visitedPaths = FindVisitedPaths(activityGrid, pathGridIndex).ToList();
            var currentDocumentIds = visitedPaths
                .Select(path => activity.UserId + "-" + path.Id.Value)
                .ToHashSet(StringComparer.Ordinal);
            var linkedDocs = await GetActivityLinkedDocuments(activity.UserId, activity.Id, cancellationToken);

            _logger.LogInformation("Activity {ActivityId} visits {PathCount} paths", activityId, visitedPaths.Count);

            if (visitedPaths.Count == 0)
            {
                await CleanupStaleVisitedPathDocuments(activity.UserId, activity.Id, currentDocumentIds, linkedDocs, cancellationToken);
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedPaths, cancellationToken);
                if (ServiceBusRescheduler.HasRealLockToken(job)) await actions.CompleteMessageAsync(job);
                return;
            }

            // Batch-read existing VisitedPath documents
            var visitedPathIds = visitedPaths
                .Select(p => activity.UserId + "-" + p.Id.Value)
                .ToList();
            var existingDocs = (await _visitedPathsCollection.GetByIdsAsync(visitedPathIds))
                .ToDictionary(d => d.Id);

            var toUpsert = new List<VisitedPath>();
            var toPatches = new List<(string Id, IReadOnlyList<PatchOperation> Operations)>();
            var partitionKey = new PartitionKey(activity.UserId);

            foreach (var pathFeature in visitedPaths)
            {
                var pathId = pathFeature.Id.Value;
                var osmHighwayId = pathFeature.Properties.TryGetValue("osmId", out var osmId)
                    ? osmId?.ToString()
                    : null;
                var documentId = activity.UserId + "-" + pathId;

                if (existingDocs.TryGetValue(documentId, out var existing))
                {
                    var patchOperations = new List<PatchOperation>();

                    if (!existing.ActivityIds.Contains(activity.Id))
                    {
                        patchOperations.Add(PatchOperation.Add("/activityIds/-", activity.Id));
                    }

                    if (string.IsNullOrWhiteSpace(existing.OsmHighwayId) && !string.IsNullOrWhiteSpace(osmHighwayId))
                    {
                        patchOperations.Add(PatchOperation.Set("/osmHighwayId", osmHighwayId));
                    }

                    if (patchOperations.Count > 0)
                    {
                        toPatches.Add((documentId, patchOperations));
                    }
                }
                else
                {
                    toUpsert.Add(new VisitedPath
                    {
                        Id = documentId,
                        UserId = activity.UserId,
                        PathId = pathId,
                        OsmHighwayId = osmHighwayId,
                        Name = pathFeature.Properties.TryGetValue("name", out var name) ? name?.ToString() : null,
                        Type = pathFeature.Properties.TryGetValue("highway", out var highway) ? highway?.ToString() : null,
                        ActivityIds = [activity.Id]
                    });
                }
            }

            if (ServiceBusRescheduler.HasRealLockToken(job))
            {
                try { await actions.RenewMessageLockAsync(job); }
                catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
                {
                    _logger.LogWarning("Lock lost before writing paths for message {MessageId}; it will be redelivered.", job.MessageId);
                }
            }
            await _visitedPathsCollection.ExecuteBatch(partitionKey, creates: toUpsert, patches: toPatches);
            await CleanupStaleVisitedPathDocuments(activity.UserId, activity.Id, currentDocumentIds, linkedDocs, cancellationToken);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedPaths, cancellationToken);
            if (ServiceBusRescheduler.HasRealLockToken(job)) await actions.CompleteMessageAsync(job);
        }
        catch (Exception ex)
        {
            if (ex is CosmosException { StatusCode: System.Net.HttpStatusCode.TooManyRequests })
            {
                ServiceBusRescheduler.RecordCosmosThrottle();
            }

            if (ServiceBusRescheduler.HasRealLockToken(job))
            {
                await ServiceBusRescheduler.HandleRetryAsync(
                    ex, actions, job, _serviceBusClient, Shared.Constants.ServiceBusConfig.CalculateVisitedPathsJobs, _logger, cancellationToken);
                return;
            }

            _logger.LogError(ex, "Failed to process visited paths for activity {ActivityId} (MessageId={MessageId}, DeliveryCount={DeliveryCount})",
                activityId, job.MessageId, job.DeliveryCount);
        }
    }

    internal static HashSet<(int, int)> BuildSpatialGrid(List<Coordinate> points)
    {
        var grid = new HashSet<(int, int)>(points.Count);
        foreach (var p in points)
            grid.Add(((int)Math.Floor(p.Lat / GridCellSize), (int)Math.Floor(p.Lng / GridCellSize)));
        return grid;
    }

    internal static PathGridIndex BuildPathGridIndex(IReadOnlyList<Feature> paths)
    {
        var cellToPathIndices = new Dictionary<(int LatCell, int LngCell), List<int>>();
        var sectionCellsByPathIndex = new PathSectionCells?[paths.Count];

        for (var i = 0; i < paths.Count; i++)
        {
            var path = paths[i];
            if (path.Geometry is not LineString line)
                continue;

            if (!line.Coordinates.Any())
                continue;

            sectionCellsByPathIndex[i] = BuildSectionCells(line.Coordinates);

            // Avoid adding the same path multiple times for duplicate coordinates in the same cell.
            var touchedCells = new HashSet<(int LatCell, int LngCell)>();
            foreach (var p in line.Coordinates)
            {
                var cell = ((int)Math.Floor(p.Latitude / GridCellSize), (int)Math.Floor(p.Longitude / GridCellSize));
                if (!touchedCells.Add(cell))
                    continue;

                if (!cellToPathIndices.TryGetValue(cell, out var pathIndices))
                {
                    pathIndices = new List<int>();
                    cellToPathIndices[cell] = pathIndices;
                }

                pathIndices.Add(i);
            }
        }

        return new PathGridIndex(paths, cellToPathIndices, sectionCellsByPathIndex);
    }

    internal static IEnumerable<Feature> FindVisitedPaths(HashSet<(int, int)> activityGrid, PathGridIndex index)
    {
        var visitedPathIndices = new HashSet<int>();

        foreach (var (latCell, lngCell) in activityGrid)
        {
            // Check the cell and all 8 neighbors to account for points near cell boundaries
            for (int dLat = -1; dLat <= 1; dLat++)
            {
                for (int dLng = -1; dLng <= 1; dLng++)
                {
                    if (!index.CellToPathIndices.TryGetValue((latCell + dLat, lngCell + dLng), out var candidates))
                        continue;

                    foreach (var pathIndex in candidates)
                    {
                        if (!visitedPathIndices.Add(pathIndex))
                            continue;

                        if (!PathIsVisitedBySections(activityGrid, index.SectionCellsByPathIndex[pathIndex]))
                            continue;

                        yield return index.Paths[pathIndex];
                    }
                }
            }
        }
    }

    private static bool PathIsVisitedBySections(HashSet<(int, int)> activityGrid, PathSectionCells? sectionCells)
    {
        if (sectionCells is null)
            return false;

        if (!SectionIsVisited(activityGrid, sectionCells.First))
            return false;
        if (!SectionIsVisited(activityGrid, sectionCells.Middle))
            return false;
        if (!SectionIsVisited(activityGrid, sectionCells.Last))
            return false;

        return true;
    }

    private static bool SectionIsVisited(HashSet<(int, int)> activityGrid, IReadOnlyList<(int LatCell, int LngCell)> sectionCells)
    {
        if (sectionCells.Count == 0)
            return true;

        foreach (var (latCell, lngCell) in sectionCells)
        {
            for (var dLat = -1; dLat <= 1; dLat++)
            {
                for (var dLng = -1; dLng <= 1; dLng++)
                {
                    if (activityGrid.Contains((latCell + dLat, lngCell + dLng)))
                        return true;
                }
            }
        }

        return false;
    }

    private static PathSectionCells BuildSectionCells(IEnumerable<Position> coordinates)
    {
        var points = coordinates.ToList();
        var first = new HashSet<(int LatCell, int LngCell)>();
        var middle = new HashSet<(int LatCell, int LngCell)>();
        var last = new HashSet<(int LatCell, int LngCell)>();

        var total = points.Count;
        for (var index = 0; index < total; index++)
        {
            var point = points[index];
            var cell = ((int)Math.Floor(point.Latitude / GridCellSize), (int)Math.Floor(point.Longitude / GridCellSize));
            var section = (index * 3) / total;

            if (section <= 0)
                first.Add(cell);
            else if (section == 1)
                middle.Add(cell);
            else
                last.Add(cell);
        }

        return new PathSectionCells([.. first], [.. middle], [.. last]);
    }

    private async Task<List<ActivitySlim>> FetchActivitySlims(List<string> ids)
    {
        const int MaxIdsPerQuery = 256;
        var all = new List<ActivitySlim>();

        var chunks = ids
            .Select((id, i) => (id, i))
            .GroupBy(x => x.i / MaxIdsPerQuery)
            .Select(g => g.Select(x => x.id).ToList());

        foreach (var chunk in chunks)
        {
            var queryText = "SELECT c.id, c.userId, c.polyline, c.summaryPolyline FROM c WHERE c.id IN (" +
                            string.Join(",", chunk.Select((_, i) => $"@id{i}")) + ")";

            var queryDef = new QueryDefinition(queryText);
            for (int i = 0; i < chunk.Count; i++)
                queryDef.WithParameter($"@id{i}", chunk[i]);

            var results = await _activitiesCollection.ExecuteQueryAsync<ActivitySlim>(queryDef);
            all.AddRange(results);
        }
        return all;
    }

    private async Task<IEnumerable<Feature>> FetchNearbyPaths(Dictionary<string, List<Coordinate>> decodedActivities)
    {
        var shardZoom = _highwaysShardFeatureClient.CanonicalZoom;
        var tileIndices = new HashSet<(int x, int y)>();
        foreach (var points in decodedActivities.Values)
        {
            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(points, shardZoom))
                tileIndices.Add(tile);
        }

        var paths = (await _highwaysShardFeatureClient.GetFeaturesForShards(tileIndices)).ToList();
        _logger.LogInformation("Found {Count} nearby paths", paths.Count);
        return paths;
    }

    private async Task<List<ActivityLinkedDocumentProjection>> GetActivityLinkedDocuments(
        string userId,
        string activityId,
        CancellationToken cancellationToken)
    {
        var query = new QueryDefinition(@"
SELECT c.id, c.activityIds
FROM c
WHERE c.userId = @userId
  AND ARRAY_CONTAINS(c.activityIds, @activityId)")
            .WithParameter("@userId", userId)
            .WithParameter("@activityId", activityId);

        return (await _visitedPathsCollection.ExecuteQueryAsync<ActivityLinkedDocumentProjection>(
            query,
            cancellationToken: cancellationToken)).ToList();
    }

    private async Task CleanupStaleVisitedPathDocuments(
        string userId,
        string activityId,
        ISet<string> currentDocumentIds,
        IEnumerable<ActivityLinkedDocumentProjection> linkedDocs,
        CancellationToken cancellationToken)
    {
        var partitionKey = new PartitionKey(userId);
        var stalePatches = new List<(string Id, IReadOnlyList<PatchOperation> Operations)>();
        var staleDeletes = new List<string>();

        foreach (var linkedDoc in linkedDocs)
        {
            if (currentDocumentIds.Contains(linkedDoc.Id))
                continue;

            var remainingActivityIds = (linkedDoc.ActivityIds ?? [])
                .Where(id => !string.Equals(id, activityId, StringComparison.Ordinal))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            if (remainingActivityIds.Count == 0)
            {
                staleDeletes.Add(linkedDoc.Id);
                continue;
            }

            stalePatches.Add((linkedDoc.Id, [PatchOperation.Set("/activityIds", remainingActivityIds)]));
        }

        if (stalePatches.Count > 0)
            await _visitedPathsCollection.ExecuteBatch(partitionKey, patches: stalePatches, cancellationToken: cancellationToken);

        foreach (var staleDelete in staleDeletes)
            await _visitedPathsCollection.DeleteDocument(staleDelete, partitionKey, cancellationToken);
    }
}
