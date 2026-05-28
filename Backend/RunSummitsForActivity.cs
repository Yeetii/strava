using System.Net;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Geo;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class RunSummitsForActivity(
    ILogger<RunSummitsForActivity> logger,
    CollectionClient<Activity> activitiesCollection,
    CollectionClient<SummitedPeak> summitedPeaksCollection,
    [FromKeyedServices(FeatureKinds.Peak)] TiledCollectionClient peaksCollection,
    ServiceBusClient serviceBusClient,
    ISummitsCalculator summitsCalculator,
    UserSyncStatusService userSyncStatusService)
{
    private readonly ServiceBusSender _activityProcessedSender = serviceBusClient.CreateSender(ServiceBusConfig.ActivityProcessed);

    [Function(nameof(RunSummitsForActivity))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "debug/summits/{activityId}")] HttpRequestData req,
        string activityId,
        CancellationToken cancellationToken)
    {
        var activity = (await activitiesCollection.GetByIdsAsync([activityId], cancellationToken)).FirstOrDefault();
        if (activity == null)
        {
            var notFound = req.CreateResponse(HttpStatusCode.NotFound);
            await notFound.WriteStringAsync($"Activity '{activityId}' was not found.", cancellationToken);
            return notFound;
        }

        var peaks = (await FetchNearbyPeaks([activity], peaksCollection)).ToList();

        if (activity.StartLatLng == null || activity.StartLatLng.Count < 2 || string.IsNullOrEmpty(activity.SummaryPolyline))
        {
            logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activity.Id);
            await SendActivityProcessedEvent(activity, []);
            await userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);

            var skipped = req.CreateResponse(HttpStatusCode.OK);
            await skipped.WriteAsJsonAsync(new
            {
                activityId = activity.Id,
                processed = true,
                skipped = true,
                reason = "missing-geodata",
                summitCount = 0
            }, cancellationToken);
            return skipped;
        }

        var summits = CalculateSummitedPeaks(activity, peaks, summitsCalculator).ToList();
        if (summits.Count == 0)
        {
            await SendActivityProcessedEvent(activity, []);
            await userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);

            var okNoSummits = req.CreateResponse(HttpStatusCode.OK);
            await okNoSummits.WriteAsJsonAsync(new
            {
                activityId = activity.Id,
                processed = true,
                skipped = false,
                summitCount = 0
            }, cancellationToken);
            return okNoSummits;
        }

        logger.LogInformation("Debug summit run: activity {ActivityId} has {SummitCount} summits", activity.Id, summits.Count);
        var activitySummitedPeaks = await UpdateSummitedPeaksDocuments(summitedPeaksCollection, activity, summits);
        await SendActivityProcessedEvent(activity, summits);
        await summitedPeaksCollection.BulkUpsert(activitySummitedPeaks, cancellationToken: cancellationToken);
        await userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);

        var ok = req.CreateResponse(HttpStatusCode.OK);
        await ok.WriteAsJsonAsync(new
        {
            activityId = activity.Id,
            processed = true,
            skipped = false,
            summitCount = summits.Count,
            summitIds = summits.Select(x => x.Id.Value).ToArray()
        }, cancellationToken);
        return ok;
    }

    private async Task SendActivityProcessedEvent(Activity activity, IEnumerable<Feature> summitedPeaks)
    {
        var processedEvent = new ActivityProcessedEvent(
            activity.Id,
            activity.UserId,
            [.. summitedPeaks.Select(x => x.Id.Value)],
            [.. summitedPeaks.Select(x => x.Properties.TryGetValue("name", out var peakName) ? peakName : "")]);

        var json = JsonSerializer.Serialize(processedEvent);
        await _activityProcessedSender.SendMessageAsync(new ServiceBusMessage(json));
    }

    private static IEnumerable<Feature> CalculateSummitedPeaks(Activity activity, IEnumerable<Feature> nearbyPeaks, ISummitsCalculator summitsCalculator)
    {
        var nearbyPoints = nearbyPeaks.Select(peak =>
        {
            var position = ((Point)peak.Geometry).Coordinates;
            return (peak.Id.Value, new Coordinate(position.Longitude, position.Latitude));
        });
        var summitedPeakIds = summitsCalculator.FindPointsNearRoute(nearbyPoints, activity.Polyline ?? activity.SummaryPolyline ?? string.Empty).ToHashSet();
        return nearbyPeaks.Where(peak => summitedPeakIds.Contains(peak.Id.Value));
    }

    private static async Task<IEnumerable<SummitedPeak>> UpdateSummitedPeaksDocuments(
        CollectionClient<SummitedPeak> summitedPeaksCollection,
        Activity activity,
        IEnumerable<Feature> summitedPeaks)
    {
        var documents = new List<SummitedPeak>();
        foreach (var peak in summitedPeaks)
        {
            var peakId = SummitsWorker.NormalizeSummitedPeakId(peak.Id);
            var documentId = SummitsWorker.BuildSummitedPeakDocumentId(activity.UserId, peak.Id);
            var partitionKey = new PartitionKey(activity.UserId);
            var summitedPeakDocument = await summitedPeaksCollection.GetByIdMaybe(documentId, partitionKey)
                ?? new SummitedPeak
                {
                    Id = documentId,
                    Name = peak.Properties.TryGetValue("name", out var peakName) ? peakName?.ToString() ?? "" : "",
                    UserId = activity.UserId,
                    PeakId = peakId,
                    Elevation = TryParseElevation(peak.Properties),
                    ActivityIds = []
                };

            summitedPeakDocument.ActivityIds.Add(activity.Id);
            documents.Add(summitedPeakDocument);
        }

        return documents;
    }

    private static float? TryParseElevation(IDictionary<string, dynamic> properties)
    {
        if (!properties.TryGetValue("elevation", out var elevationValue) || elevationValue == null)
            return null;

        if (elevationValue is float f)
            return f;
        if (elevationValue is double d)
            return (float)d;
        if (elevationValue is int i)
            return i;
        if (elevationValue is long l)
            return (float)l;
        if (elevationValue is string s && float.TryParse(s, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var parsed))
            return parsed;
        if (elevationValue is JsonElement je && je.ValueKind == JsonValueKind.Number)
        {
            if (je.TryGetSingle(out var single))
                return single;
            if (je.TryGetDouble(out var dbl))
                return (float)dbl;
        }

        return null;
    }

    private static async Task<IEnumerable<Feature>> FetchNearbyPeaks(IEnumerable<Activity> activities, TiledCollectionClient peaksCollection)
    {
        var tileIndices = new HashSet<(int x, int y)>();
        foreach (var activity in activities)
        {
            var polyline = activity.SummaryPolyline;
            if (string.IsNullOrEmpty(polyline))
                continue;

            var tiles = SlippyTileCalculator.TileIndicesByLine(GeoSpatialFunctions.DecodePolyline(polyline));
            foreach (var tile in tiles)
                tileIndices.Add(tile);
        }

        var nearbyPeaks = await peaksCollection.FetchByTiles(tileIndices);
        return nearbyPeaks.Select(x => x.ToFeature());
    }
}