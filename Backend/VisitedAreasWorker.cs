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

public class VisitedAreasWorker(
    ILogger<VisitedAreasWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<VisitedArea> _visitedAreasCollection,
    [FromKeyedServices(FeatureKinds.ProtectedArea)] TiledCollectionClient _protectedAreasCollection,
    AdminBoundariesCollectionClient _adminBoundariesCollection,
    ServiceBusClient serviceBusClient)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    private const int AreaTileZoom = 8;
    private const int AdminLevelRegion = 4;

    private sealed record VisitedAreaCandidate(
        string AreaId,
        string Name,
        string AreaType,
        string? Wikidata,
        string? WikimediaCommons);

    [Function(nameof(VisitedAreasWorker))]
    public async Task Run(
        [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.CalculateVisitedAreasJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] jobs,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var ids = jobs.Select(x => x.Body.ToString());
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(a.Polyline ?? a.SummaryPolyline))
            .ToList();

        var nearbyAreas = (await FetchNearbyAreas(activitiesList)).ToList();

        var processingTasks = jobs.Select(job => ProcessJob(job, actions, activitiesList, nearbyAreas, cancellationToken));
        await Task.WhenAll(processingTasks);
    }

    private async Task ProcessJob(
        ServiceBusReceivedMessage job,
        ServiceBusMessageActions actions,
        List<Activity> activitiesList,
        List<StoredFeature> nearbyAreas,
        CancellationToken cancellationToken)
    {
        var activityId = job.Body.ToString();
        try
        {
            var activity = activitiesList.FirstOrDefault(a => a.Id == activityId);

            if (activity == null || string.IsNullOrWhiteSpace(activity.Polyline ?? activity.SummaryPolyline))
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activityId);
                await actions.CompleteMessageAsync(job);
                return;
            }

            var activityPoints = GeoSpatialFunctions.DecodePolyline(activity.Polyline ?? activity.SummaryPolyline ?? string.Empty).ToList();
            var nearbyRegions = (await FetchVisitedRegionSummaries(activityPoints, cancellationToken)).ToList();
            var visitedAreas = FindVisitedAreas(activityPoints, nearbyAreas, nearbyRegions).ToList();

            _logger.LogInformation("Activity {ActivityId} visits {AreaCount} areas", activityId, visitedAreas.Count);

            if (visitedAreas.Count == 0)
            {
                await actions.CompleteMessageAsync(job);
                return;
            }

            var partitionKey = new PartitionKey(activity.UserId);
            var documentIds = visitedAreas
                .Select(area => activity.UserId + "-" + area.AreaId)
                .ToList();
            var existingDocs = (await _visitedAreasCollection.GetByIdsAsync(documentIds, cancellationToken))
                .ToDictionary(doc => doc.Id, StringComparer.Ordinal);

            var toCreate = new List<VisitedArea>();
            var toPatch = new List<(string Id, IReadOnlyList<PatchOperation> Operations)>();
            foreach (var area in visitedAreas)
            {
                var documentId = activity.UserId + "-" + area.AreaId;
                if (existingDocs.TryGetValue(documentId, out var existing))
                {
                    if (!existing.ActivityIds.Contains(activity.Id))
                    {
                        toPatch.Add((documentId, [
                            PatchOperation.Add("/activityIds/-", activity.Id)
                        ]));
                    }
                    continue;
                }

                toCreate.Add(new VisitedArea
                {
                    Id = documentId,
                    UserId = activity.UserId,
                    AreaId = area.AreaId,
                    Name = area.Name,
                    AreaType = area.AreaType,
                    Wikidata = area.Wikidata,
                    WikimediaCommons = area.WikimediaCommons,
                    ActivityIds = [activity.Id]
                });
            }

            await actions.RenewMessageLockAsync(job);
            await _visitedAreasCollection.ExecuteBatch(partitionKey, creates: toCreate, patches: toPatch, cancellationToken: cancellationToken);
            await actions.CompleteMessageAsync(job);
        }
        catch (Exception ex)
        {
            await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                ex, actions, job, _serviceBusClient, Shared.Constants.ServiceBusConfig.CalculateVisitedAreasJobs, _logger, cancellationToken);
            return;
        }
    }

    private static IEnumerable<VisitedAreaCandidate> FindVisitedAreas(
        List<Coordinate> activityPoints,
        IEnumerable<StoredFeature> areas,
        IEnumerable<StoredFeatureSummary> regions)
    {
        foreach (var area in areas)
        {
            if (ActivityVisitsArea(activityPoints, area.Geometry))
            {
                var areaId = area.LogicalId;
                yield return new VisitedAreaCandidate(
                    areaId,
                    GetPropertyValue(area.Properties, "name") ?? areaId,
                    GetPropertyValue(area.Properties, "areaType") ?? "protected_area",
                    GetPropertyValue(area.Properties, "wikidata"),
                    GetPropertyValue(area.Properties, "wikimedia_commons"));
            }
        }

        foreach (var region in regions)
        {
            var regionId = region.LogicalId;
            yield return new VisitedAreaCandidate(
                regionId,
                GetPropertyValue(region.Properties, "name") ?? regionId,
                "region",
                GetPropertyValue(region.Properties, "wikidata"),
                GetPropertyValue(region.Properties, "wikimedia_commons"));
        }
    }

    private static string? GetPropertyValue(IDictionary<string, dynamic> properties, string key)
        => properties.TryGetValue(key, out var value) ? value?.ToString() : null;

    private static bool ActivityVisitsArea(List<Coordinate> activityPoints, Geometry geometry)
    {
        foreach (var idx in GetSampledIndices(activityPoints.Count))
        {
            if (RouteFeatureMatcher.IsPointInGeometry(activityPoints[idx], geometry))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Yields point indices in a sampled order to enable early termination:
    /// first every 10th point (0, 10, 20, ...), then offset by 5 (5, 15, 25, ...),
    /// then all remaining unvisited indices in order.
    /// </summary>
    private static IEnumerable<int> GetSampledIndices(int count)
    {
        const int step = 10;
        var visited = new HashSet<int>();

        for (int i = 0; i < count; i += step)
        {
            visited.Add(i);
            yield return i;
        }

        for (int i = 5; i < count; i += step)
        {
            if (visited.Add(i))
                yield return i;
        }

        for (int i = 0; i < count; i++)
        {
            if (visited.Add(i))
                yield return i;
        }
    }

    private async Task<IEnumerable<StoredFeature>> FetchNearbyAreas(IEnumerable<Activity> activities)
    {
        var areaTileIndices = new HashSet<(int x, int y)>();
        foreach (var activity in activities)
        {
            var polyline = activity.SummaryPolyline ?? activity.Polyline;
            if (string.IsNullOrEmpty(polyline))
                continue;

            var points = GeoSpatialFunctions.DecodePolyline(polyline);
            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(points, AreaTileZoom))
                areaTileIndices.Add(tile);
        }

        var areas = (await _protectedAreasCollection.FetchByTiles(areaTileIndices, AreaTileZoom, followPointers: true)).ToList();
        _logger.LogInformation("Found {Count} nearby protected areas", areas.Count);
        return areas;
    }

    private async Task<IEnumerable<StoredFeatureSummary>> FetchVisitedRegionSummaries(
        List<Coordinate> activityPoints,
        CancellationToken cancellationToken)
    {
        var sampledPoints = GetSampledIndices(activityPoints.Count)
            .Select(index => activityPoints[index])
            .ToList();

        var regions = (await _adminBoundariesCollection.FindBoundarySummariesContainingAnyPoint(
            sampledPoints, AdminLevelRegion, cancellationToken)).ToList();
        _logger.LogInformation("Found {Count} visited admin regions via geospatial lookup", regions.Count);
        return regions;
    }
}
