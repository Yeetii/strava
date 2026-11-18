using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Spatial;
using Shared.Services.Postgres.Spatial.Ingestion;
using Shared.Geo;
using Shared.Constants;
using Shared.Geo.SummitsCalculator;
using BAMCIS.GeoJSON;

namespace Backend;

// Input format, given an activityId:
// 11808921572
public class SummitsWorker(ILogger<SummitsWorker> _logger,
    IActivityRepository _activityRepository,
    IActivitySummitRepository _activitySummitRepository,
    IPeakRepository _peaksRepository,
    ISummitsCalculator _summitsCalculator,
    UserSyncStatusService _userSyncStatusService,
    IOsmFeatureIngestionService _osmFeatureIngestion)
{
    private const int MaxDegreeOfParallelism = 4;

    // Matches ISummitsCalculator.FindPointsNearRoute's default threshold: a peak is a summit
    // when its point lies within this many metres of any decoded route coordinate. Fetching
    // candidates with the same threshold around each route point keeps the result identical
    // to the old tile-based fetch while letting PostGIS do the spatial prefilter.
    private const int PeakProximityMeters = 50;

    [Function("SummitsWorker")]
    public async Task Run(
        [QueueTrigger("calculateSummitsJobs", IsBatched = true, AutoCompleteMessages = false)] QueueMessage[] jobs, QueueMessageActions actions, CancellationToken cancellationToken)
    {

        var jobIds = jobs.Select(x => x.Body.ToString()).ToList();
        var activities = await _activityRepository.GetByIds(jobIds, cancellationToken);
        var activitiesById = activities.ToDictionary(x => x.Id, StringComparer.Ordinal);
        var peaks = (await FetchNearbyPeaks(activitiesById.Values, cancellationToken)).ToList();

        // Renew locks after the potentially slow peak fetch before per-job processing
        var realJobs = jobs.Where(QueueMessageGuard.HasLockToken).ToList();
        await Task.WhenAll(realJobs.Select(async j =>
        {
            try { await actions.RenewMessageLockAsync(j); }
            catch (Exception ex) when (ex is QueueException { Reason: QueueExceptionReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
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

    private async Task ProcessSummitJob(QueueMessage job, QueueMessageActions actions, List<Feature> peaks, Activity activity, CancellationToken cancellationToken)
    {
        try
        {
            if (activity.StartLatLng == null || activity.StartLatLng.Count < 2 || string.IsNullOrEmpty(activity.SummaryPolyline))
            {
                _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activity.Id);
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);
                await actions.CompleteMessageAsync(job);
                return;
            }
            var summits = CalculateSummitedPeaks(activity, peaks).ToList();
            if (summits.Count == 0)
            {
                // No summits means this activity should have no rows in activity_summits.
                await _activitySummitRepository.RemoveByActivity(activity.Id, cancellationToken);
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);
                if (QueueMessageGuard.HasLockToken(job))
                    try { await actions.RenewMessageLockAsync(job); } catch (Exception ex) when (ex is QueueException { Reason: QueueExceptionReason.MessageLockLost } || ex.Message.Contains("MessageLockLost")) { }
                await actions.CompleteMessageAsync(job);
                return;
            }
            _logger.LogInformation("Activity {ActivityId} has {SummitCount} summits", activity.Id, summits.Count);

            var links = summits
                .Select(summit => new SummitLink(
                    activity.Id,
                    activity.UserId,
                    NormalizeSummitedPeakId(summit.Id),
                    DateTime.UtcNow))
                .ToList();

            // Replace the activity's previous links so stale summits are removed. AddRange is
            // idempotent (ON CONFLICT DO NOTHING); the FK cascade removes rows when the
            // activity itself is deleted, so no manual domain-document cleanup is needed.
            await _activitySummitRepository.RemoveByActivity(activity.Id, cancellationToken);
            await _activitySummitRepository.AddRange(links, cancellationToken);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);
            if (QueueMessageGuard.HasLockToken(job))
                try { await actions.RenewMessageLockAsync(job); } catch (Exception ex) when (ex is QueueException { Reason: QueueExceptionReason.MessageLockLost } || ex.Message.Contains("MessageLockLost")) { }
            await actions.CompleteMessageAsync(job);
        }
        catch (Exception ex)
        {
            throw;

        }
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

    internal static string BuildSummitedPeakDocumentId(string userId, FeatureId peakId)
        => $"{userId}-{NormalizeSummitedPeakId(peakId)}";

    internal static string NormalizeSummitedPeakId(FeatureId peakId)
        => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, peakId.Value);

    private async Task<IEnumerable<Feature>> FetchNearbyPeaks(IEnumerable<Activity> activities, CancellationToken cancellationToken)
    {
        var decodedRoutes = activities
            .Select(activity => activity.Polyline ?? activity.SummaryPolyline)
            .Where(polyline => !string.IsNullOrEmpty(polyline))
            .Select(polyline => GeoSpatialFunctions.DecodePolyline(polyline!)
                .DistinctBy(point => (point.Lat, point.Lng))
                .ToList())
            .ToList();

        // Warm the PostGIS peaks table from Overpass before any spatial lookup so a freshly
        // migrated database does not silently yield zero candidates.
        var routePoints = decodedRoutes.SelectMany(points => points).ToList();
        var ingestion = await _osmFeatureIngestion.EnsurePeaksAsync(routePoints, cancellationToken)
            ?? OsmIngestionResult.Empty;
        _logger.LogInformation(
            "OSM peak ingestion for {RouteCount} routes: tilesQueried={TilesQueried}, fetched={Fetched}, upserted={Upserted}, failed={Failed}",
            decodedRoutes.Count,
            ingestion.TilesQueried,
            ingestion.FeaturesFetched,
            ingestion.FeaturesUpserted,
            ingestion.TilesFailed);

        var peaksById = new Dictionary<string, StoredFeature>(StringComparer.Ordinal);
        foreach (var points in decodedRoutes)
        {
            foreach (var point in points)
            {
                var nearby = await _peaksRepository.FindWithinRadius(point, PeakProximityMeters, cancellationToken);
                foreach (var peak in nearby)
                    peaksById[peak.Id] = peak;
            }
        }
        _logger.LogInformation("Found {count} nearby peaks", peaksById.Count);
        return peaksById.Values.Select(x => x.ToFeature());
    }
}
