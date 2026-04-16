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
    AdminBoundariesCollectionClient _adminBoundariesCollection)
{
    private const int AreaTileZoom = 8;
    private const int RegionTileZoom = 6;
    private const int AdminLevelRegion = 4;

    [Function(nameof(VisitedAreasWorker))]
    public async Task Run(
        [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.CalculateVisitedAreasJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] jobs,
        ServiceBusMessageActions actions)
    {
        var ids = jobs.Select(x => x.Body.ToString());
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(a.Polyline ?? a.SummaryPolyline))
            .ToList();

        var nearbyAreas = (await FetchNearbyAreas(activitiesList)).ToList();

        var processingTasks = jobs.Select(job => ProcessJob(job, actions, activitiesList, nearbyAreas));
        await Task.WhenAll(processingTasks);
    }

    private async Task ProcessJob(
        ServiceBusReceivedMessage job,
        ServiceBusMessageActions actions,
        List<Activity> activitiesList,
        List<StoredFeature> nearbyAreas)
    {
        var activityId = job.Body.ToString();
        var activity = activitiesList.FirstOrDefault(a => a.Id == activityId);

        if (activity == null || string.IsNullOrWhiteSpace(activity.Polyline ?? activity.SummaryPolyline))
        {
            _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activityId);
            await actions.CompleteMessageAsync(job);
            return;
        }

        var activityPoints = GeoSpatialFunctions.DecodePolyline(activity.Polyline ?? activity.SummaryPolyline ?? string.Empty).ToList();
        var visitedAreas = FindVisitedAreas(activityPoints, nearbyAreas).ToList();

        _logger.LogInformation("Activity {ActivityId} visits {AreaCount} areas", activityId, visitedAreas.Count);

        var documents = new List<VisitedArea>();
        foreach (var area in visitedAreas)
        {
            var areaId = area.LogicalId;
            var documentId = activity.UserId + "-" + areaId;
            var partitionKey = new PartitionKey(activity.UserId);
            var areaType = area.Kind == FeatureKinds.AdminBoundary
                ? "region"
                : (area.Properties.TryGetValue("areaType", out var existingAreaType) ? existingAreaType?.ToString() ?? "protected_area" : "protected_area");
            var doc = await _visitedAreasCollection.GetByIdMaybe(documentId, partitionKey)
                ?? new VisitedArea
                {
                    Id = documentId,
                    UserId = activity.UserId,
                    AreaId = areaId,
                    Name = area.Properties.TryGetValue("name", out var name) ? name?.ToString() ?? areaId : areaId,
                    AreaType = areaType,
                    Wikidata = area.Properties.TryGetValue("wikidata", out var wikidata) ? wikidata?.ToString() : null,
                    WikimediaCommons = area.Properties.TryGetValue("wikimedia_commons", out var wmc) ? wmc?.ToString() : null,
                    ActivityIds = []
                };
            doc.ActivityIds.Add(activity.Id);
            documents.Add(doc);
        }

        await actions.RenewMessageLockAsync(job);
        await _visitedAreasCollection.BulkUpsert(documents);
        await actions.CompleteMessageAsync(job);
    }

    private static IEnumerable<StoredFeature> FindVisitedAreas(List<Coordinate> activityPoints, IEnumerable<StoredFeature> areas)
    {
        foreach (var area in areas)
        {
            if (ActivityVisitsArea(activityPoints, area.Geometry))
                yield return area;
        }
    }

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
        var regionTileIndices = new HashSet<(int x, int y)>();
        foreach (var activity in activities)
        {
            var polyline = activity.SummaryPolyline ?? activity.Polyline;
            if (string.IsNullOrEmpty(polyline))
                continue;

            var points = GeoSpatialFunctions.DecodePolyline(polyline);
            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(points, AreaTileZoom))
                areaTileIndices.Add(tile);
            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(points, RegionTileZoom))
                regionTileIndices.Add(tile);
        }

        var areasTask = _protectedAreasCollection.FetchByTiles(areaTileIndices, AreaTileZoom, followPointers: true);
        var regionsTask = _adminBoundariesCollection.FetchByTiles(regionTileIndices, AdminLevelRegion, RegionTileZoom, followPointers: true);
        var (areas, regions) = (await areasTask, await regionsTask);

        var allFeatures = areas.Concat(regions).ToList();
        _logger.LogInformation("Found {Count} nearby areas and regions", allFeatures.Count);
        return allFeatures;
    }
}
