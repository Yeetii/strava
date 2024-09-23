using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared;
using Microsoft.Azure.Cosmos;

namespace Backend;

public class SummitsWorker(ILogger<SummitsWorker> _logger,
    CollectionClient<StoredFeature> _peaksCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    UserAuthenticationService _userAuthService)
{
    [Function("SummitsWorker")]
    [SignalROutput(HubName = "peakshunters")]
    public async Task<IEnumerable<SignalRMessageAction>> Run(
        [ServiceBusTrigger("calculateSummitsJobs", Connection = "ServicebusConnection")] string json,
        [CosmosDBInput(
        databaseName: "%CosmosDb%",
        containerName: "%ActivitiesContainer%",
        Connection  = "CosmosDBConnection",
        Id = "{activityId}",
        PartitionKey = "{activityId}"
        )] Activity activity)
    {
        if (activity.StartLatLng == null || activity.StartLatLng.Count < 2)
        {
            _logger.LogInformation("Skipping activity {ActivityId} since it has no start location", activity.Id);
            return await PublishJobDoneEvent(_logger, activity, []);
        }
        var startLocation = new Coordinate(activity.StartLatLng[1], activity.StartLatLng[0]);
        var activityLength = (int)Math.Ceiling(activity.Distance ?? 0);
        var nearbyPeaks = await _peaksCollection.GeoSpatialFetch(startLocation, activityLength);
        var nearbyPoints = nearbyPeaks.Select(peak => (peak.Id, Coordinate.ParseGeoJsonCoordinate(peak.Geometry.Coordinates)));
        var summitedPeakIds = GeoSpatialFunctions.FindPointsIntersectingLine(nearbyPoints, activity.Polyline ?? activity.SummaryPolyline ?? string.Empty);
        var summitedPeaks = nearbyPeaks.Where(peak => summitedPeakIds.Contains(peak.Id));
        await WriteToSummitedPeaks(_summitedPeaksCollection, activity, summitedPeaks);
        return await PublishJobDoneEvent(_logger, activity, summitedPeaks);
    }

    private static async Task WriteToSummitedPeaks(CollectionClient<SummitedPeak> _summitedPeaksCollection, Activity activity, IEnumerable<StoredFeature> summitedPeaks)
    {
        foreach (var peak in summitedPeaks)
        {
            var documentId = activity.UserId + "-" + peak.Id;
            var partitionKey = new PartitionKey(activity.UserId);
            var summitedPeakDocument = await _summitedPeaksCollection.GetByIdMaybe(documentId, partitionKey)
                ?? new SummitedPeak
                {
                    Id = documentId,
                    Name = peak.Properties.TryGetValue("name", out var peakName) ? peakName as string : "",
                    UserId = activity.UserId,
                    PeakId = peak.Id,
                    Elevation = peak.Properties.TryGetValue("elevation", out var elevation) ? float.Parse(elevation as string) : null,
                    ActivityIds = []
                };
            summitedPeakDocument.ActivityIds.Add(activity.Id);
            await _summitedPeaksCollection.UpsertDocument(summitedPeakDocument);
        }
    }

    private record SummitsEvent(string ActivityId, string[] SummitedPeakIds, string[] SummitedPeakNames, bool SummitedAnyPeaks);
    private async Task<IEnumerable<SignalRMessageAction>> PublishJobDoneEvent(ILogger<SummitsWorker> _logger, Activity activity, IEnumerable<StoredFeature> summitedPeaks)
    {
        var userId = activity.UserId;
        var sessionIds = await _userAuthService.GetUsersActiveSessions(userId);
        var summitedPeakIds = summitedPeaks.Select(peak => peak.Id).ToArray();
        var summitedPeakNames = summitedPeaks.Select(peak => peak.Properties.TryGetValue("name", out var peakName) ? peakName as string : "").ToArray();
        var anySummitedPeaks = summitedPeakIds.Length != 0;
        var summitsEvent = new SummitsEvent(activity.Id, summitedPeakIds, summitedPeakNames, anySummitedPeaks);

        var signalRMessages = new List<SignalRMessageAction>();

        foreach (var sessionId in sessionIds)
        {
            if (!summitedPeakIds.Any())
            {
                _logger.LogInformation("Found no peaks for activity {ActivityId}", activity.Id);
                signalRMessages.Add(new SignalRMessageAction("summitsEvents")
                {
                    Arguments = [summitsEvent],
                    UserId = sessionId,
                });
            }
            _logger.LogInformation("Found {AmnPeaks} summited peaks for activity {ActivityId}", summitedPeakIds.Length, activity.Id);
            signalRMessages.Add(new SignalRMessageAction("summitsEvents")
            {
                Arguments = [summitsEvent],
                UserId = sessionId,
            });
        }
        return signalRMessages;
    }
}
