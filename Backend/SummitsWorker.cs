using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Shared.Geo;
using Shared.Constants;
using Azure.Messaging.ServiceBus;
using System.Text.Json;
using System.Globalization;
using Shared.Geo.SummitsCalculator;
using BAMCIS.GeoJSON;

namespace Backend;

// Input format, given an activityId: 
// 11808921572
public class SummitsWorker(ILogger<SummitsWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    [FromKeyedServices(FeatureKinds.Peak)] TiledCollectionClient _peaksCollection,
    ServiceBusClient serviceBusClient,
    ISummitsCalculator _summitsCalculator,
    UserSyncStatusService _userSyncStatusService)
{
    readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    readonly ServiceBusSender _sbSender = serviceBusClient.CreateSender("activityprocessed");
    private const int MaxDegreeOfParallelism = 4;

    [Function("SummitsWorker")]
    public async Task Run(
        [ServiceBusTrigger("calculateSummitsJobs", Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)] ServiceBusReceivedMessage[] jobs, ServiceBusMessageActions actions, CancellationToken cancellationToken)
    {
        var jobIds = jobs.Select(x => x.Body.ToString()).ToList();
        var activities = await _activitiesCollection.GetByIdsAsync(jobIds, cancellationToken);
        var activitiesById = activities.ToDictionary(x => x.Id, StringComparer.Ordinal);
        var peaks = (await FetchNearbyPeaks(activitiesById.Values)).ToList();

        // Renew locks after the potentially slow peak fetch before per-job processing
        var realJobs = jobs.Where(ServiceBusCosmosRetryHelper.HasRealLockToken).ToList();
        await Task.WhenAll(realJobs.Select(async j =>
        {
            try { await actions.RenewMessageLockAsync(j); }
            catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                _logger.LogWarning("Lock lost before processing for message {MessageId}; it will be redelivered.", j.MessageId);
            }
        }));

        await Parallel.ForEachAsync(
            jobs,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = MaxDegreeOfParallelism,
                CancellationToken = cancellationToken
            },
            async (job, ct) =>
            {
                var activityId = job.Body.ToString();
                if (!activitiesById.TryGetValue(activityId, out var activity))
                {
                    _logger.LogWarning(
                        "No activity document found for calculateSummits job {MessageId} (activity id: {ActivityId}); completing message.",
                        job.MessageId,
                        activityId);
                    await actions.CompleteMessageAsync(job, ct);
                    return;
                }

                await ProcessSummitJob(job, actions, peaks, activity, ct);
            });
    }
    private async Task ProcessSummitJob(ServiceBusReceivedMessage job, ServiceBusMessageActions actions, List<Feature> peaks, Activity activity, CancellationToken cancellationToken)
    {
        try
        {
            if (activity.StartLatLng == null || activity.StartLatLng.Count < 2 || string.IsNullOrEmpty(activity.SummaryPolyline))
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activity.Id);
                await SendActivityProcessedEvent(activity, []);
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);
                await actions.CompleteMessageAsync(job);
                return;
            }
            var summits = CalculateSummitedPeaks(activity, peaks).ToList();
            if (summits.Count == 0)
            {
                await SendActivityProcessedEvent(activity, []);
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);
                if (ServiceBusCosmosRetryHelper.HasRealLockToken(job))
                    try { await actions.RenewMessageLockAsync(job); } catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost")) { }
                await actions.CompleteMessageAsync(job);
                return;
            }
            _logger.LogInformation("Activity {ActivityId} has {SummitCount} summits", activity.Id, summits.Count);
            var activitySummitedPeaks = await UpdateSummitedPeaksDocuments(_summitedPeaksCollection, activity, summits);
            await SendActivityProcessedEvent(activity, summits);
            await _summitedPeaksCollection.BulkUpsert(activitySummitedPeaks);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);
            if (ServiceBusCosmosRetryHelper.HasRealLockToken(job))
                try { await actions.RenewMessageLockAsync(job); } catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost")) { }
            await actions.CompleteMessageAsync(job);
        }
        catch (Exception ex)
        {
            await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                ex, actions, job, _serviceBusClient, ServiceBusConfig.CalculateSummitsJobs, _logger, cancellationToken);
            return;
        }
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
        var summitedPeakList = summitedPeaks.ToList();
        if (summitedPeakList.Count == 0)
            return [];

        var existingDocs = (await _summitedPeaksCollection.GetByIdsAsync(
            summitedPeakList.Select(peak => BuildSummitedPeakDocumentId(activity.UserId, peak.Id))))
            .ToDictionary(doc => doc.Id, StringComparer.Ordinal);

        var documents = new List<SummitedPeak>();
        foreach (var peak in summitedPeakList)
        {
            var peakId = NormalizeSummitedPeakId(peak.Id);
            var documentId = BuildSummitedPeakDocumentId(activity.UserId, peak.Id);
            var summitedPeakDocument = existingDocs.TryGetValue(documentId, out var existing)
                ? existing
                : new SummitedPeak
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

    internal static string BuildSummitedPeakDocumentId(string userId, FeatureId peakId)
        => $"{userId}-{NormalizeSummitedPeakId(peakId)}";

    private static float? TryParseElevation(IDictionary<string, dynamic> properties)
    {
        if (!properties.TryGetValue("elevation", out var elevationValue) || elevationValue == null)
            return null;

        // Handle different types that might come from Cosmos/JSON
        if (elevationValue is float f)
            return f;
        if (elevationValue is double d)
            return (float)d;
        if (elevationValue is int i)
            return i;
        if (elevationValue is long l)
            return (float)l;
        if (elevationValue is string s && float.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
            return parsed;
        if (elevationValue is System.Text.Json.JsonElement je && je.ValueKind == System.Text.Json.JsonValueKind.Number)
        {
            if (je.TryGetSingle(out var single))
                return single;
            if (je.TryGetDouble(out var dbl))
                return (float)dbl;
        }

        return null;
    }

    internal static string NormalizeSummitedPeakId(FeatureId peakId)
        => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, peakId.Value);

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
