using Stigar.Core.Models;
using Microsoft.Extensions.Logging;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;
using Stigar.Core.Geo;
using Stigar.Core.Constants;
using Stigar.Core.Geo.SummitsCalculator;
using Stigar.Core.Jobs;
using BAMCIS.GeoJSON;

namespace Stigar.Workers;

// Input format, given an activityId:
// 11808921572
public class SummitsWorker(ILogger<SummitsWorker> _logger,
    IActivityRepository _activityRepository,
    IActivitySummitRepository _activitySummitRepository,
    IPeakRepository _peaksRepository,
    ISummitsCalculator _summitsCalculator,
    UserSyncStatusService _userSyncStatusService,
    IOsmFeatureIngestionService _osmFeatureIngestion) : ISummitsJob
{
    // Matches ISummitsCalculator.FindPointsNearRoute's default threshold: a peak is a summit
    // when its point lies within this many metres of any decoded route coordinate. Fetching
    // candidates with the same threshold around each route point keeps the result identical
    // to the old tile-based fetch while letting PostGIS do the spatial prefilter.
    private const int PeakProximityMeters = 50;

    public async Task ExecuteAsync(ActivityIdJob job, CancellationToken cancellationToken)
    {
        var activityId = job.ActivityId;
        var activities = await _activityRepository.GetByIds([activityId], cancellationToken);
        var activity = activities.FirstOrDefault();
        if (activity is null)
        {
            _logger.LogWarning(
                "No activity document found for calculateSummits job (activity id: {ActivityId}); stopping.",
                activityId);
            return;
        }

        var peaks = (await FetchNearbyPeaks([activity], cancellationToken)).ToList();
        await ProcessSummitJob(peaks, activity, job.Epoch, cancellationToken);
    }

    private async Task ProcessSummitJob(List<Feature> peaks, Activity activity, long epoch, CancellationToken cancellationToken)
    {
        if (activity.StartLatLng == null || activity.StartLatLng.Count < 2 || string.IsNullOrEmpty(activity.SummaryPolyline))
        {
            _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activity.Id);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, epoch, cancellationToken);
            return;
        }
        var summits = CalculateSummitedPeaks(activity, peaks).ToList();
        if (summits.Count == 0)
        {
            // No summits means this activity should have no rows in activity_summits.
            await _activitySummitRepository.RemoveByActivity(activity.Id, cancellationToken);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, epoch, cancellationToken);
            return;
        }
        _logger.LogInformation("Activity {ActivityId} has {SummitCount} summits", activity.Id, summits.Count);

        var links = summits
            .Select(summit => new SummitLink(
                activity.Id,
                activity.UserId,
                ToStoredPeakId(summit.Id),
                DateTime.UtcNow))
            .ToList();

        // Replace the activity's previous links so stale summits are removed. AddRange is
        // idempotent (ON CONFLICT DO NOTHING); the FK cascade removes rows when the
        // activity itself is deleted, so no manual domain-document cleanup is needed.
        await _activitySummitRepository.RemoveByActivity(activity.Id, cancellationToken);
        await _activitySummitRepository.AddRange(links, cancellationToken);
        await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, epoch, cancellationToken);
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
        => PeakIdRules.BuildSummitedPeakDocumentId(userId, peakId);

    internal static string ToStoredPeakId(FeatureId peakId)
        => PeakIdRules.ToStoredPeakId(peakId);

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

        var tileIndices = routePoints
            .Select(point => SlippyTileCalculator.WGS84ToTileIndex(point, 11))
            .Distinct()
            .ToList();

        if (tileIndices.Count == 0)
            return [];

        var peaks = await _peaksRepository.GetByTiles(tileIndices, cancellationToken);
        _logger.LogInformation("Found {count} nearby peaks across {tileCount} tiles", peaks.Count, tileIndices.Count);
        return peaks.Select(x => x.ToFeature());
    }
}
