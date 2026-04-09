using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;

namespace Backend;

public class VisitedPathsWorker(
    ILogger<VisitedPathsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<VisitedPath> _visitedPathsCollection,
    PathsCollectionClient _pathsCollection)
{
    private const int PathTileZoom = 11;

    // 50m ≈ 0.00045° – use a slightly larger threshold for a rough but fast check
    private const double ProximityThresholdDegrees = 0.0005;

    [Function(nameof(VisitedPathsWorker))]
    public async Task Run(
        [ServiceBusTrigger("calculateVisitedPathsJobs", Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] jobs,
        ServiceBusMessageActions actions)
    {
        var ids = jobs.Select(x => x.Body.ToString());
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(a.Polyline ?? a.SummaryPolyline))
            .ToList();

        var nearbyPaths = (await FetchNearbyPaths(activitiesList)).ToList();

        var processingTasks = jobs.Select(job => ProcessJob(job, actions, activitiesList, nearbyPaths));
        await Task.WhenAll(processingTasks);
    }

    private async Task ProcessJob(
        ServiceBusReceivedMessage job,
        ServiceBusMessageActions actions,
        List<Activity> activitiesList,
        List<Feature> nearbyPaths)
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
        var visitedPathIds = FindVisitedPaths(activityPoints, nearbyPaths).ToList();

        _logger.LogInformation("Activity {ActivityId} visits {PathCount} paths", activityId, visitedPathIds.Count);

        var documents = new List<VisitedPath>();
        foreach (var pathId in visitedPathIds)
        {
            var documentId = activity.UserId + "-" + pathId;
            var partitionKey = new PartitionKey(activity.UserId);
            var doc = await _visitedPathsCollection.GetByIdMaybe(documentId, partitionKey)
                ?? new VisitedPath
                {
                    Id = documentId,
                    UserId = activity.UserId,
                    PathId = pathId,
                    ActivityIds = []
                };
            doc.ActivityIds.Add(activity.Id);
            documents.Add(doc);
        }

        await actions.RenewMessageLockAsync(job);
        await _visitedPathsCollection.BulkUpsert(documents);
        await actions.CompleteMessageAsync(job);
    }

    private static IEnumerable<string> FindVisitedPaths(List<Coordinate> activityPoints, IEnumerable<Feature> paths)
    {
        foreach (var path in paths)
        {
            if (path.Geometry is not LineString line)
                continue;

            var pathPoints = line.Coordinates
                .Select(p => new Coordinate(p.Longitude, p.Latitude))
                .ToList();

            if (ActivityVisitsPath(activityPoints, pathPoints))
                yield return path.Id.Value;
        }
    }

    private static bool ActivityVisitsPath(List<Coordinate> activityPoints, List<Coordinate> pathPoints)
    {
        foreach (var ap in activityPoints)
        {
            foreach (var pp in pathPoints)
            {
                if (Math.Abs(ap.Lat - pp.Lat) < ProximityThresholdDegrees
                    && Math.Abs(ap.Lng - pp.Lng) < ProximityThresholdDegrees)
                {
                    return true;
                }
            }
        }
        return false;
    }

    private async Task<IEnumerable<Feature>> FetchNearbyPaths(IEnumerable<Activity> activities)
    {
        var tileIndices = new HashSet<(int x, int y)>();
        foreach (var activity in activities)
        {
            var polyline = activity.SummaryPolyline ?? activity.Polyline;
            if (string.IsNullOrEmpty(polyline))
                continue;

            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(GeoSpatialFunctions.DecodePolyline(polyline), PathTileZoom))
                tileIndices.Add(tile);
        }

        var pathCollection = await _pathsCollection.FetchByTiles(tileIndices, PathTileZoom);
        _logger.LogInformation("Found {Count} nearby paths", pathCollection.Features.Count());
        return pathCollection.Features;
    }
}
