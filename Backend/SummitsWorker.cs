using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Microsoft.Azure.Cosmos;
using Shared.Geo;
using Azure.Messaging.ServiceBus;
using System.Text.Json;
using Shared.Geo.SummitsCalculator;
using BAMCIS.GeoJSON;

namespace Backend;

// Input format, given an activityId: 
// 11808921572
public class SummitsWorker(ILogger<SummitsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    PeaksCollectionClient _peaksCollection,
    ServiceBusClient serviceBusClient,
    ISummitsCalculator _summitsCalculator)
{
    readonly ServiceBusSender _sbSender = serviceBusClient.CreateSender("activityprocessed");

    [Function("SummitsWorker")]
    public async Task Run(
        [ServiceBusTrigger("calculateSummitsJobs", Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)] ServiceBusReceivedMessage[] jobs, ServiceBusMessageActions actions)
    {
        var ids = jobs.Select(x => x.Body.ToString());
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var peaks = (await FetchNearbyPeaks(activities)).ToList();
        var processingTasks = activities.Select(activity => ProcessSummitJob(_logger, _summitedPeaksCollection, jobs.First(x => x.Body.ToString() == activity.Id), actions, peaks, activity));
        await Task.WhenAll(processingTasks.ToArray());
    }
    private async Task ProcessSummitJob(ILogger<SummitsWorker> _logger, CollectionClient<SummitedPeak> _summitedPeaksCollection, ServiceBusReceivedMessage job, ServiceBusMessageActions actions, List<Feature> peaks, Activity activity)
    {
        if (activity.StartLatLng == null || activity.StartLatLng.Count < 2 || string.IsNullOrEmpty(activity.SummaryPolyline))
        {
            _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activity.Id);
            await SendActivityProcessedEvent(activity, []);
            await actions.CompleteMessageAsync(job);
            return;
        }
        var summits = CalculateSummitedPeaks(activity, peaks).ToList();
        if (summits.Count == 0)
        {
            await SendActivityProcessedEvent(activity, []);
            await actions.RenewMessageLockAsync(job);
            await actions.CompleteMessageAsync(job);
            return;
        }
        _logger.LogInformation("Activity {ActivityId} has {SummitCount} summits", activity.Id, summits.Count);
        var activitySummitedPeaks = await UpdateSummitedPeaksDocuments(_summitedPeaksCollection, activity, summits);
        await SendActivityProcessedEvent(activity, summits);
        await _summitedPeaksCollection.BulkUpsert(activitySummitedPeaks);
        await actions.RenewMessageLockAsync(job);
        await actions.CompleteMessageAsync(job);
        return;
    }

    private async Task SendActivityProcessedEvent(Activity activity, IEnumerable<Feature> summitedPeaks)
    {
        var processedEvent = new ActivityProcessedEvent(activity.Id, activity.UserId, [.. summitedPeaks.Select(x => x.Id.Value)], [.. summitedPeaks.Select(x => x.Properties.TryGetValue("name", out var peakName) ? peakName : "")]);
        var json = JsonSerializer.Serialize(processedEvent);
        await _sbSender.SendMessageAsync(new ServiceBusMessage(json));
    }

    private IEnumerable<Feature> CalculateSummitedPeaks(Activity activity, IEnumerable<Feature> nearbyPeaks)
    {
        var nearbyPoints = nearbyPeaks.Select(peak =>
        {
            var position = ((Point)peak.Geometry).Coordinates;
            return (peak.Id.Value, new Coordinate(position.Longitude, position.Latitude));
        });
        var summitedPeakIds = _summitsCalculator.FindPointsNearRoute(nearbyPoints, activity.Polyline ?? activity.SummaryPolyline ?? string.Empty).ToHashSet();
        var summitedPeaks = nearbyPeaks.Where(peak => summitedPeakIds.Contains(peak.Id.Value));
        return summitedPeaks;
    }

    private static async Task<IEnumerable<SummitedPeak>> UpdateSummitedPeaksDocuments(CollectionClient<SummitedPeak> _summitedPeaksCollection, Activity activity, IEnumerable<Feature> summitedPeaks)
    {
        var documents = new List<SummitedPeak>();
        foreach (var peak in summitedPeaks)
        {
            var documentId = activity.UserId + "-" + peak.Id;
            var partitionKey = new PartitionKey(activity.UserId);
            var summitedPeakDocument = await _summitedPeaksCollection.GetByIdMaybe(documentId, partitionKey)
                ?? new SummitedPeak
                {
                    Id = documentId,
                    Name = peak.Properties.TryGetValue("name", out var peakName) ? peakName : "",
                    UserId = activity.UserId,
                    PeakId = peak.Id.Value,
                    Elevation = peak.Properties.TryGetValue("elevation", out var elevation) ? float.Parse(elevation) : null,
                    ActivityIds = []
                };
            summitedPeakDocument.ActivityIds.Add(activity.Id);
            documents.Add(summitedPeakDocument);
        }
        return documents;
    }

    private async Task<IEnumerable<Feature>> FetchNearbyPeaks(IEnumerable<Activity> activities)
    {
        var tileIndices = new HashSet<(int x, int y)>();
        foreach (var activity in activities)
        {
            var polyline = activity.SummaryPolyline;
            if (string.IsNullOrEmpty(polyline))
                continue;
            var tiles = SlippyTileCalculator.TileIndicesByLine(GeoSpatialFunctions.DecodePolyline(polyline));
            foreach (var tile in tiles)
            {
                tileIndices.Add(tile);
            }
        }
        var nearbyPeaks = await _peaksCollection.FetchByTiles(tileIndices);
        _logger.LogInformation("Found {count} nearby peaks", nearbyPeaks.Count());
        return nearbyPeaks.Select(x => x.ToFeature());
    }
}
