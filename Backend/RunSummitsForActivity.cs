using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Geo;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Spatial;
using Shared.Services.Postgres.Spatial.Ingestion;

namespace Backend;

public class RunSummitsForActivity(
    ILogger<RunSummitsForActivity> logger,
    IActivityRepository activitiesRepository,
    IActivitySummitRepository activitySummitRepository,
    IPeakRepository peaksRepository,
    ISummitsCalculator summitsCalculator,
    UserSyncStatusService userSyncStatusService,
    IOsmFeatureIngestionService osmFeatureIngestion)
{
    // Matches ISummitsCalculator.FindPointsNearRoute's default threshold.
    private const int PeakProximityMeters = 50;

    [Function(nameof(RunSummitsForActivity))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "debug/summits/{activityId}")] HttpRequestData req,
        string activityId,
        CancellationToken cancellationToken)
    {
        var activity = await activitiesRepository.GetById(activityId, cancellationToken);
        if (activity == null)
        {
            var notFound = req.CreateResponse(HttpStatusCode.NotFound);
            await notFound.WriteStringAsync($"Activity '{activityId}' was not found.", cancellationToken);
            return notFound;
        }

        var peaks = (await FetchNearbyPeaks([activity], cancellationToken)).ToList();

        if (activity.StartLatLng == null || activity.StartLatLng.Count < 2 || string.IsNullOrEmpty(activity.SummaryPolyline))
        {
            logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activity.Id);
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
        var links = summits
            .Select(summit => new SummitLink(
                activity.Id,
                activity.UserId,
                SummitsWorker.NormalizeSummitedPeakId(summit.Id),
                DateTime.UtcNow));
        await activitySummitRepository.AddRange(links, cancellationToken);
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

    private async Task<IEnumerable<Feature>> FetchNearbyPeaks(
        IEnumerable<Activity> activities,
        CancellationToken cancellationToken)
    {
        var decodedRoutes = activities
            .Select(activity => activity.Polyline ?? activity.SummaryPolyline)
            .Where(polyline => !string.IsNullOrEmpty(polyline))
            .Select(polyline => GeoSpatialFunctions.DecodePolyline(polyline!)
                .DistinctBy(point => (point.Lat, point.Lng))
                .ToList())
            .ToList();

        var routePoints = decodedRoutes.SelectMany(points => points).ToList();
        var ingestion = await osmFeatureIngestion.EnsurePeaksAsync(routePoints, cancellationToken)
            ?? OsmIngestionResult.Empty;
        logger.LogInformation(
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
                var nearby = await peaksRepository.FindWithinRadius(point, PeakProximityMeters, cancellationToken);
                foreach (var peak in nearby)
                    peaksById[peak.Id] = peak;
            }
        }
        return peaksById.Values.Select(x => x.ToFeature());
    }
}
