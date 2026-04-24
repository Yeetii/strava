using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;

namespace Backend;

public class VisitedPathsWorker(
    ILogger<VisitedPathsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<VisitedPath> _visitedPathsCollection,
    [FromKeyedServices(FeatureKinds.Path)] TiledCollectionClient _pathsCollection,
    ServiceBusClient serviceBusClient)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    private const int PathTileZoom = 11;

    // 50m ≈ 0.00045° – use a slightly larger threshold for a rough but fast check
    private const double ProximityThresholdDegrees = 0.0005;

    // Grid cell size matches the proximity threshold for O(1) lookups
    private const double GridCellSize = ProximityThresholdDegrees;

    private record ActivitySlim(string Id, string UserId, string? Polyline, string? SummaryPolyline);

    [Function(nameof(VisitedPathsWorker))]
    public async Task Run(
        [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.CalculateVisitedPathsJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] jobs,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var ids = jobs.Select(x => x.Body.ToString()).ToList();
        var activities = await FetchActivitySlims(ids);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(a.Polyline ?? a.SummaryPolyline))
            .ToList();

        // Decode polylines once and reuse
        var decodedActivities = activitiesList.ToDictionary(
            a => a.Id,
            a => GeoSpatialFunctions.DecodePolyline(a.Polyline ?? a.SummaryPolyline).ToList());

        var nearbyPaths = (await FetchNearbyPaths(decodedActivities)).ToList();

        // Renew locks after the potentially slow shared data fetch
        var realJobs = jobs.Where(ServiceBusCosmosRetryHelper.HasRealLockToken).ToList();
        await Task.WhenAll(realJobs.Select(j => actions.RenewMessageLockAsync(j)));

        foreach (var job in jobs)
            await ProcessJob(job, actions, activitiesList, decodedActivities, nearbyPaths, cancellationToken);
    }

    private async Task ProcessJob(
        ServiceBusReceivedMessage job,
        ServiceBusMessageActions actions,
        List<ActivitySlim> activitiesList,
        Dictionary<string, List<Coordinate>> decodedActivities,
        List<Feature> nearbyPaths,
        CancellationToken cancellationToken)
    {
        var activityId = job.Body.ToString();
        try
        {
            var activity = activitiesList.FirstOrDefault(a => a.Id == activityId);

            if (activity == null || !decodedActivities.TryGetValue(activityId, out var activityPoints) || activityPoints.Count == 0)
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activityId);
                if (ServiceBusCosmosRetryHelper.HasRealLockToken(job)) await actions.CompleteMessageAsync(job);
                return;
            }

            var grid = BuildSpatialGrid(activityPoints);
            var visitedPaths = FindVisitedPaths(grid, nearbyPaths).ToList();

            _logger.LogInformation("Activity {ActivityId} visits {PathCount} paths", activityId, visitedPaths.Count);

            if (visitedPaths.Count == 0)
            {
                if (ServiceBusCosmosRetryHelper.HasRealLockToken(job)) await actions.CompleteMessageAsync(job);
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
                var documentId = activity.UserId + "-" + pathId;

                if (existingDocs.TryGetValue(documentId, out var existing))
                {
                    if (!existing.ActivityIds.Contains(activity.Id))
                    {
                        toPatches.Add((documentId, [
                            PatchOperation.Add("/activityIds/-", activity.Id)
                        ]));
                    }
                }
                else
                {
                    toUpsert.Add(new VisitedPath
                    {
                        Id = documentId,
                        UserId = activity.UserId,
                        PathId = pathId,
                        Name = pathFeature.Properties.TryGetValue("name", out var name) ? name?.ToString() : null,
                        Type = pathFeature.Properties.TryGetValue("highway", out var highway) ? highway?.ToString() : null,
                        ActivityIds = [activity.Id]
                    });
                }
            }

            if (ServiceBusCosmosRetryHelper.HasRealLockToken(job)) await actions.RenewMessageLockAsync(job);
            await _visitedPathsCollection.ExecuteBatch(partitionKey, creates: toUpsert, patches: toPatches);
            if (ServiceBusCosmosRetryHelper.HasRealLockToken(job)) await actions.CompleteMessageAsync(job);
        }
        catch (Exception ex)
        {
            if (ServiceBusCosmosRetryHelper.HasRealLockToken(job))
            {
                await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                    ex, actions, job, _serviceBusClient, Shared.Constants.ServiceBusConfig.CalculateVisitedPathsJobs, _logger, cancellationToken);
                return;
            }

            _logger.LogError(ex, "Failed to process visited paths for activity {ActivityId} (MessageId={MessageId}, DeliveryCount={DeliveryCount})",
                activityId, job.MessageId, job.DeliveryCount);
        }
    }

    private static HashSet<(int, int)> BuildSpatialGrid(List<Coordinate> points)
    {
        var grid = new HashSet<(int, int)>(points.Count);
        foreach (var p in points)
            grid.Add(((int)Math.Floor(p.Lat / GridCellSize), (int)Math.Floor(p.Lng / GridCellSize)));
        return grid;
    }

    private static IEnumerable<Feature> FindVisitedPaths(HashSet<(int, int)> activityGrid, IEnumerable<Feature> paths)
    {
        foreach (var path in paths)
        {
            if (path.Geometry is not LineString line)
                continue;

            if (PathIntersectsGrid(activityGrid, line.Coordinates))
                yield return path;
        }
    }

    private static bool PathIntersectsGrid(HashSet<(int, int)> activityGrid, IEnumerable<Position> pathCoords)
    {
        foreach (var p in pathCoords)
        {
            int latCell = (int)Math.Floor(p.Latitude / GridCellSize);
            int lngCell = (int)Math.Floor(p.Longitude / GridCellSize);

            // Check the cell and all 8 neighbors to account for points near cell boundaries
            for (int dLat = -1; dLat <= 1; dLat++)
            {
                for (int dLng = -1; dLng <= 1; dLng++)
                {
                    if (activityGrid.Contains((latCell + dLat, lngCell + dLng)))
                        return true;
                }
            }
        }
        return false;
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
        var tileIndices = new HashSet<(int x, int y)>();
        foreach (var points in decodedActivities.Values)
        {
            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(points, PathTileZoom))
                tileIndices.Add(tile);
        }

        var paths = (await _pathsCollection.FetchByTiles(tileIndices, PathTileZoom, followPointers: true)).ToList();
        _logger.LogInformation("Found {Count} nearby paths", paths.Count);
        return paths.Select(p => p.ToFeature());
    }
}
