using Microsoft.AspNetCore.Mvc;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Geo;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Spatial;
using Shared.Services.Postgres.Spatial.Ingestion;

namespace API.Endpoints.Backend;

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

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/debug/summits/{activityId}",
            (HttpContext context, [FromServices] RunSummitsForActivity endpoint, string activityId) =>
                endpoint.Handle(context, activityId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string activityId, CancellationToken cancellationToken)
    {
        var activity = await activitiesRepository.GetById(activityId, cancellationToken);
        if (activity == null)
            return TypedResults.NotFound($"Activity '{activityId}' was not found.");

        var peaks = (await FetchNearbyPeaks([activity], cancellationToken)).ToList();

        if (activity.StartLatLng == null || activity.StartLatLng.Count < 2 || string.IsNullOrEmpty(activity.SummaryPolyline))
        {
            logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activity.Id);
            await userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);

            return TypedResults.Ok(new
            {
                activityId = activity.Id,
                processed = true,
                skipped = true,
                reason = "missing-geodata",
                summitCount = 0
            });
        }

        var summits = CalculateSummitedPeaks(activity, peaks, summitsCalculator).ToList();
        if (summits.Count == 0)
        {
            await userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);

            return TypedResults.Ok(new
            {
                activityId = activity.Id,
                processed = true,
                skipped = false,
                summitCount = 0
            });
        }

        logger.LogInformation("Debug summit run: activity {ActivityId} has {SummitCount} summits", activity.Id, summits.Count);
        var links = summits
            .Select(summit => new SummitLink(
                activity.Id,
                activity.UserId,
                PeakIdRules.Normalize(summit.Id),
                DateTime.UtcNow))
            .ToList();
        await activitySummitRepository.RemoveByActivity(activity.Id, cancellationToken);
        await activitySummitRepository.AddRange(links, cancellationToken);
        await userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.SummitedPeaks, cancellationToken);

        return TypedResults.Ok(new
        {
            activityId = activity.Id,
            processed = true,
            skipped = false,
            summitCount = summits.Count,
            summitIds = summits.Select(x => x.Id.Value).ToArray()
        });
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
            .Select(activity => activity.SummaryPolyline ?? activity.Polyline)
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

        var tileIndices = routePoints
            .Select(point => SlippyTileCalculator.WGS84ToTileIndex(point, 11))
            .Distinct()
            .ToList();

        if (tileIndices.Count == 0)
            return [];

        var peaks = await peaksRepository.GetByTiles(tileIndices, cancellationToken);
        logger.LogInformation("Found {count} nearby peaks across {tileCount} tiles", peaks.Count, tileIndices.Count);
        return peaks.Select(x => x.ToFeature());
    }
}
