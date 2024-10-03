using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared;
using Microsoft.Azure.Cosmos;
using Shared.Helpers;
using System.Collections.Immutable;
using Azure.Messaging.ServiceBus;
using System.Text.Json;

namespace Backend;

// Input format, given an activityId: 
// 11808921572
public class SummitsWorker(ILogger<SummitsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    PeaksCollectionClient _peaksCollection,
    ServiceBusClient serviceBusClient)
{
    readonly ServiceBusSender _sbSender = serviceBusClient.CreateSender("activityprocessed");

    [Function("SummitsWorker")]
    public async Task Run(
        [ServiceBusTrigger("calculateSummitsJobs", Connection = "ServicebusConnection", IsBatched = true)] ServiceBusReceivedMessage[] jobs, ServiceBusMessageActions actions)
    {
        var ids = jobs.Select(x => x.Body.ToString());
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var peaks = (await FetchNearbyPeaks(activities)).ToList();

        await Parallel.ForEachAsync(activities, async (activity, token) =>
        {

            if (activity.StartLatLng == null || activity.StartLatLng.Count < 2)
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no start location", activity.Id);
                await SendActivityProcessedEvent(activity, [], token);
                await actions.CompleteMessageAsync(jobs.First(x => x.Body.ToString() == activity.Id), token);
                return;
            }
            var summitedPeaks = CalculateSummitedPeaks(activity, peaks);
            await WriteToSummitedPeaks(_summitedPeaksCollection, activity, summitedPeaks);
            await SendActivityProcessedEvent(activity, summitedPeaks, token);
            await actions.CompleteMessageAsync(jobs.First(x => x.Body.ToString() == activity.Id), token);
        });
        return;
    }

    private async Task SendActivityProcessedEvent(Activity activity, IEnumerable<Feature> summitedPeaks, CancellationToken token)
    {
        var processedEvent = new ActivityProcessedEvent(activity.Id, activity.UserId, summitedPeaks.Select(x => x.Id).ToArray(), summitedPeaks.Select(x => x.Properties.TryGetValue("name", out var peakName) ? peakName : "").ToArray());
        var json = JsonSerializer.Serialize(processedEvent);
        await _sbSender.SendMessageAsync(new ServiceBusMessage(json), token);
    }

    private IEnumerable<Feature> CalculateSummitedPeaks(Activity activity, IEnumerable<Feature> nearbyPeaks)
    {
        var startLocation = new Coordinate(activity.StartLatLng[1], activity.StartLatLng[0]);
        var activityLength = (int)Math.Ceiling(activity.Distance ?? 0);

        var filteredPeaks = nearbyPeaks.Where(peak => GeoSpatialFunctions.DistanceTo(Coordinate.ParseGeoJsonCoordinate(peak.Geometry.Coordinates), startLocation) < activityLength).ToList();
        _logger.LogInformation("Calculating summits on {amnPeaks} peaks, for activity {activityId}, at {coord} with length {dist}m", filteredPeaks.Count, activity.Id, startLocation.ToList(), activityLength);
        var nearbyPoints = filteredPeaks.Select(peak => (peak.Id, Coordinate.ParseGeoJsonCoordinate(peak.Geometry.Coordinates)));
        var summitedPeakIds = GeoSpatialFunctions.FindPointsIntersectingLine(nearbyPoints, activity.Polyline ?? activity.SummaryPolyline ?? string.Empty);
        var summitedPeaks = nearbyPeaks.Where(peak => summitedPeakIds.Contains(peak.Id));
        return summitedPeaks;
    }

    private static async Task WriteToSummitedPeaks(CollectionClient<SummitedPeak> _summitedPeaksCollection, Activity activity, IEnumerable<Feature> summitedPeaks)
    {
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
                    PeakId = peak.Id,
                    Elevation = peak.Properties.TryGetValue("elevation", out var elevation) ? float.Parse(elevation) : null,
                    ActivityIds = []
                };
            summitedPeakDocument.ActivityIds.Add(activity.Id);
            await _summitedPeaksCollection.UpsertDocument(summitedPeakDocument);
        }
    }

    private async Task<IEnumerable<Feature>> FetchNearbyPeaks(IEnumerable<Activity> activities)
    {
        var tileIndices = new HashSet<(int x, int y)>();
        foreach (var activity in activities)
        {
            var polyline = activity.SummaryPolyline;
            if (string.IsNullOrEmpty(polyline))
                continue;
            var tiles = SlippyTileCalculator.TileIndicesByLine(GeoSpatialFunctions.DecodePolyLine(polyline));
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
