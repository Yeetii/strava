using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared;
using Microsoft.Azure.Cosmos;
using Shared.Helpers;
using System.Collections.Immutable;
using System.Net.Http.Json;
using static Backend.QueueSummitJobs;
using System.Drawing;

namespace Backend;

// Input format: 
// {"activityId": "11808921572", "userId": "192"}
public class SummitsWorker(ILogger<SummitsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    UserAuthenticationService _userAuthService,
    IHttpClientFactory _httpClientFactory)
{
    readonly HttpClient _apiClient = _httpClientFactory.CreateClient("apiClient");

    [Function("SummitsWorker")]
    [SignalROutput(HubName = "peakshunters")]
    public async Task<IEnumerable<SignalRMessageAction>> Run(
        [ServiceBusTrigger("calculateSummitsJobs", Connection = "ServicebusConnection", IsBatched = true)] IEnumerable<CalculateSummitJob> trigger)
    {
        var ids = trigger.Select(x => x.ActivityId);
        var activities = await _activitiesCollection.GetByIdsAsync(ids);
        var peaks = await FetchNearbyPeaks(activities);

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
        _logger.LogInformation("Calculating summits on {amnPeaks}", filteredPeaks.Count);
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
        var tileIndices = new List<Point>();
        foreach (var activity in activities)
        {
            if (activity.StartLatLng == null || activity.StartLatLng.Count < 2)
            {
                continue;
            }
            var startLocation = new Coordinate(activity.StartLatLng[1], activity.StartLatLng[0]);
            var activityLength = (int)Math.Ceiling(activity.Distance ?? 0);

            var tiles = SlippyTileCalculator.TileIndicesByRadius(startLocation, activityLength);
            tileIndices.AddRange(tiles);
        }
        var nearbyPeaks = await FetchPeaksFromTiles(tileIndices.Distinct());
        _logger.LogInformation("Found {count} nearby peaks", nearbyPeaks.Count());
        return nearbyPeaks;
    }

    private async Task<IEnumerable<Feature>> FetchPeaksFromTiles(IEnumerable<Point> tiles)
    {
        var peakFetchTasks = tiles.Select(i => _apiClient.GetAsync($"peaks/{i.X}/{i.Y}"));
        var peakResponses = await Task.WhenAll(peakFetchTasks);
        var peakCollections = await Task.WhenAll(peakResponses.Select(response => response.Content.ReadFromJsonAsync<FeatureCollection>()));
        return peakCollections.SelectMany(collection => collection?.Features ?? []);
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
