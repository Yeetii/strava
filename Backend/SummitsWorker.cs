using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared;
using Microsoft.Azure.Cosmos;
using Shared.Helpers;
using System.Collections.Immutable;
using static Backend.QueueSummitJobs;

namespace Backend;

// Input format: 
// {"activityId": "11808921572", "userId": "192"}
public class SummitsWorker(ILogger<SummitsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    PeaksCollectionClient _peaksCollection,
    UserAuthenticationService _userAuthService)
{

    [Function("SummitsWorker")]
    [SignalROutput(HubName = "peakshunters")]
    public async Task<IEnumerable<SignalRMessageAction>> Run(
        [ServiceBusTrigger("calculateSummitsJobs", Connection = "ServicebusConnection", IsBatched = true)] IEnumerable<CalculateSummitJob> trigger)
    {
        var ids = trigger.Select(x => x.ActivityId);
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var peaks = (await FetchNearbyPeaks(activities)).ToList();

        var outputs = new List<Task<IEnumerable<SignalRMessageAction>>>();

        Parallel.ForEach(activities, async activity =>
        {
            if (activity.StartLatLng == null || activity.StartLatLng.Count < 2)
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no start location", activity.Id);
                outputs.Add(PublishJobDoneEvent(activity, []));
                return;
            }
            var summitedPeaks = CalculateSummitedPeaks(activity, peaks);
            await WriteToSummitedPeaks(_summitedPeaksCollection, activity, summitedPeaks);
            outputs.Add(PublishJobDoneEvent(activity, summitedPeaks));
        });
        return (await Task.WhenAll(outputs)).SelectMany(output => output);
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

    private record SummitsEvent(string ActivityId, string[] SummitedPeakIds, string[] SummitedPeakNames, bool SummitedAnyPeaks);
    private async Task<IEnumerable<SignalRMessageAction>> PublishJobDoneEvent(Activity activity, IEnumerable<Feature> summitedPeaks)
    {
        var userId = activity.UserId;
        var sessionIds = await _userAuthService.GetUsersActiveSessions(userId);
        var summitedPeakIds = summitedPeaks.Select(peak => peak.Id).ToArray();
        var summitedPeakNames = summitedPeaks.Select(peak => peak.Properties.TryGetValue("name", out var peakName) ? peakName as string : "").ToArray();
        var anySummitedPeaks = summitedPeakIds.Length != 0;
        var summitsEvent = new SummitsEvent(activity.Id, summitedPeakIds, summitedPeakNames, anySummitedPeaks);

        var signalRMessages = new List<SignalRMessageAction>();

        if (anySummitedPeaks)
        {
            _logger.LogInformation("Found {AmnPeaks} summited peaks for activity {ActivityId}", summitedPeakIds.Length, activity.Id);
        }
        else
        {
            _logger.LogInformation("Found no peaks for activity {ActivityId}", activity.Id);
        }

        foreach (var sessionId in sessionIds)
        {
            if (summitedPeakIds.Length == 0)
            {
                signalRMessages.Add(new SignalRMessageAction("summitsEvents")
                {
                    Arguments = [summitsEvent],
                    UserId = sessionId,
                });
            }
            signalRMessages.Add(new SignalRMessageAction("summitsEvents")
            {
                Arguments = [summitsEvent],
                UserId = sessionId,
            });
        }
        return signalRMessages;
    }
}
