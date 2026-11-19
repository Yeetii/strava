using Microsoft.Extensions.Logging;
using Stigar.Core.Geo;
using Stigar.Core.Models;
using Stigar.Core.Services;
using BAMCIS.GeoJSON;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;
using System.Globalization;
using Stigar.Core.Jobs;

namespace Stigar.Workers;

public class VisitedAreasWorker(
    ILogger<VisitedAreasWorker> _logger,
    IActivityRepository _activityRepository,
    IActivityVisitedAreaRepository _visitedAreaRepository,
    IProtectedAreaRepository _protectedAreasRepository,
    IAdminBoundaryRepository _adminBoundariesRepository,
    UserSyncStatusService _userSyncStatusService,
    IOsmFeatureIngestionService _osmFeatureIngestion) : IVisitedAreasJob
{
    private const int AdminLevelRegion = 4;
    private const int MaxAdminBoundaryLookupPoints = 120;

    private sealed record VisitedAreaCandidate(
        string AreaId,
        string Name,
        string AreaType,
        string? Wikidata,
        string? WikimediaCommons);

    public async Task ExecuteAsync(ActivityIdJob job, CancellationToken cancellationToken)
    {
        var activityId = job.ActivityId;
        var activities = await _activityRepository.GetByIds([activityId], cancellationToken);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(GetRoutePolyline(a)))
            .ToList();

        var nearbyAreas = (await FetchNearbyAreas(activitiesList, cancellationToken)).ToList();
        await ProcessJob(activityId, activitiesList, nearbyAreas, cancellationToken);
    }

    private async Task ProcessJob(
        string activityId,
        List<Activity> activitiesList,
        List<StoredFeature> nearbyAreas,
        CancellationToken cancellationToken)
    {
        var activity = activitiesList.FirstOrDefault(a => a.Id == activityId);

        var routePolyline = activity is null ? null : GetRoutePolyline(activity);

        if (activity == null || string.IsNullOrWhiteSpace(routePolyline))
        {
            _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activityId);
            if (activity != null)
            {
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedAreas, cancellationToken);
            }
            return;
        }

        var activityPoints = GeoSpatialFunctions.DecodePolyline(routePolyline).ToList();
        _logger.LogDebug("Activity {ActivityId} decoded {PointCount} route points", activityId, activityPoints.Count);

        var boundaryLookupPolyline = GetBoundaryLookupPolyline(activity) ?? routePolyline;
        var boundaryLookupPoints = string.Equals(boundaryLookupPolyline, routePolyline, StringComparison.Ordinal)
            ? activityPoints
            : GeoSpatialFunctions.DecodePolyline(boundaryLookupPolyline).ToList();
        _logger.LogDebug(
            "Activity {ActivityId} using {LookupPointCount} points for admin boundary lookup",
            activityId,
            boundaryLookupPoints.Count);

        var nearbyRegions = (await FetchAdminRegionsForActivity(boundaryLookupPoints, cancellationToken)).ToList();

        if (nearbyRegions.Count == 0)
        {
            _logger.LogWarning(
                "No admin boundary found for activity {ActivityId} after lookup.",
                activityId);
        }

        foreach (var region in nearbyRegions)
        {
            if (string.IsNullOrWhiteSpace(GetCountryIso2FromRegion(region.Properties)))
            {
                _logger.LogDebug(
                    "No country could be derived for visited region {RegionId} on activity {ActivityId}. Expected ISO 3166-2 style properties (e.g. RO-BR).",
                    region.LogicalId,
                    activityId);
            }
        }

        var visitedAreas = FindVisitedProtectedAreas(activityPoints, nearbyAreas)
            .Concat(FindVisitedRegionAreas(nearbyRegions))
            .Concat(FindVisitedCountriesFromRegions(nearbyRegions))
            .ToList();

        var visitedCountryCount = visitedAreas.Count(a => string.Equals(a.AreaType, "country", StringComparison.Ordinal));
        _logger.LogInformation(
            "Visited areas summary for activity {ActivityId}: nearby protected={NearbyProtectedCount}, visited admin regions={VisitedRegionCount}, visited countries={VisitedCountryCount}, total visited areas={AreaCount}",
            activityId,
            nearbyAreas.Count,
            nearbyRegions.Count,
            visitedCountryCount,
            visitedAreas.Count);

        if (visitedAreas.Count == 0)
        {
            // No visited areas means this activity should have no rows in activity_visited_areas.
            await _visitedAreaRepository.RemoveByActivity(activity.Id, cancellationToken);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedAreas, cancellationToken);
            return;
        }

        var links = visitedAreas
            .Select(area => new VisitedAreaLink(activity.Id, activity.UserId, area.AreaId, DateTime.UtcNow))
            .ToList();

        _logger.LogInformation(
            "Writing visited areas for activity {ActivityId}: links={LinkCount}, areaIds=[{AreaIds}]",
            activityId,
            links.Count,
            string.Join(", ", visitedAreas.Select(area => area.AreaId)));
        // Replace the activity's previous links, then insert the current ones. The join
        // table holds only (activity, user, area); the area's descriptive properties come
        // from the protected_areas/admin_boundaries domain tables. FK cascade removes rows
        // when the activity itself is deleted.
        await _visitedAreaRepository.RemoveByActivity(activity.Id, cancellationToken);
        await _visitedAreaRepository.AddRange(links, cancellationToken);
        _logger.LogInformation("Batch write complete for activity {ActivityId}", activityId);
        await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedAreas, cancellationToken);
    }

    private static IEnumerable<VisitedAreaCandidate> FindVisitedProtectedAreas(
        List<Coordinate> activityPoints,
        IEnumerable<StoredFeature> areas)
    {
        foreach (var area in areas)
        {
            if (ActivityVisitsArea(activityPoints, area.Geometry))
            {
                var areaId = area.LogicalId;
                yield return new VisitedAreaCandidate(
                    areaId,
                    GetPropertyValue(area.Properties, "name") ?? areaId,
                    GetPropertyValue(area.Properties, "areaType") ?? "protected_area",
                    GetPropertyValue(area.Properties, "wikidata"),
                    GetPropertyValue(area.Properties, "wikimedia_commons"));
            }
        }
    }

    private static IEnumerable<VisitedAreaCandidate> FindVisitedRegionAreas(IEnumerable<StoredFeatureSummary> regions)
    {
        foreach (var region in regions)
        {
            var regionId = region.LogicalId;
            yield return new VisitedAreaCandidate(
                regionId,
                GetPropertyValue(region.Properties, "name") ?? regionId,
                "region",
                GetPropertyValue(region.Properties, "wikidata"),
                GetPropertyValue(region.Properties, "wikimedia_commons"));
        }
    }

    private static IEnumerable<VisitedAreaCandidate> FindVisitedCountriesFromRegions(IEnumerable<StoredFeatureSummary> regions)
    {
        var visitedCountryIso2Codes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var region in regions)
        {
            var countryIso2 = GetCountryIso2FromRegion(region.Properties);
            if (!string.IsNullOrWhiteSpace(countryIso2))
            {
                visitedCountryIso2Codes.Add(countryIso2);
            }
        }

        foreach (var countryIso2 in visitedCountryIso2Codes.Order(StringComparer.Ordinal))
        {
            yield return new VisitedAreaCandidate(
                $"country:{countryIso2}",
                GetCountryName(countryIso2),
                "country",
                null,
                null);
        }
    }

    private static string? GetPropertyValue(IDictionary<string, dynamic> properties, string key)
    {
        if (properties.TryGetValue(key, out var value))
            return value?.ToString();

        foreach (var kvp in properties)
        {
            if (string.Equals(kvp.Key, key, StringComparison.OrdinalIgnoreCase))
                return kvp.Value?.ToString();
        }

        return null;
    }

    private static string? GetCountryIso2FromRegion(IDictionary<string, dynamic> regionProperties)
    {
        var iso3166_2 = GetPropertyValue(regionProperties, "iso3166-2")
            ?? GetPropertyValue(regionProperties, "ISO3166-2")
            ?? GetPropertyValue(regionProperties, "isO3166-2");

        if (!string.IsNullOrWhiteSpace(iso3166_2))
        {
            var separatorIndex = iso3166_2.IndexOf('-');
            var countryPart = separatorIndex > 0
                ? iso3166_2[..separatorIndex]
                : iso3166_2;

            var normalized = countryPart.Trim().ToUpperInvariant();
            if (normalized.Length == 2)
                return normalized;
        }

        var countryCode = GetPropertyValue(regionProperties, "countryCode");
        if (!string.IsNullOrWhiteSpace(countryCode))
        {
            var normalized = countryCode.Trim().ToUpperInvariant();
            if (normalized.Length == 2)
                return normalized;
        }

        return null;
    }

    private static string GetCountryName(string iso2)
    {
        try
        {
            return new RegionInfo(iso2).EnglishName;
        }
        catch (ArgumentException)
        {
            // Fallback for non-standard or unknown country codes.
            return iso2;
        }
    }

    private static bool ActivityVisitsArea(List<Coordinate> activityPoints, Geometry geometry)
    {
        foreach (var idx in GetSampledIndices(activityPoints.Count))
        {
            if (RouteFeatureMatcher.IsPointInGeometry(activityPoints[idx], geometry))
                return true;
        }
        return false;
    }

    private static IEnumerable<int> GetSampledIndices(int count)
    {
        const int step = 10;
        var visited = new HashSet<int>();

        for (int i = 0; i < count; i += step)
        {
            visited.Add(i);
            yield return i;
        }

        for (int i = 5; i < count; i += step)
        {
            if (visited.Add(i))
                yield return i;
        }

        for (int i = 0; i < count; i++)
        {
            if (visited.Add(i))
                yield return i;
        }
    }

    private async Task<IEnumerable<StoredFeature>> FetchNearbyAreas(
        IEnumerable<Activity> activities,
        CancellationToken cancellationToken)
    {
        // PostGIS performs the containment test (ST_Intersects) that the old worker ran
        // locally over tile-fetched documents. Query with the same decoded route points
        // so the candidate set matches the final FindVisitedProtectedAreas result.
        var decodedRoutes = activities
            .Select(activity => activity.SummaryPolyline ?? activity.Polyline)
            .Where(polyline => !string.IsNullOrEmpty(polyline))
            .Select(polyline => GeoSpatialFunctions.DecodePolyline(polyline!)
                .DistinctBy(point => (point.Lat, point.Lng))
                .ToList())
            .ToList();

        var routePoints = decodedRoutes.SelectMany(points => points).ToList();
        var ingestion = await _osmFeatureIngestion.EnsureProtectedAreasAsync(routePoints, cancellationToken)
            ?? OsmIngestionResult.Empty;
        _logger.LogInformation(
            "OSM protected-area ingestion for {RouteCount} routes: tilesQueried={TilesQueried}, fetched={Fetched}, upserted={Upserted}, failed={Failed}",
            decodedRoutes.Count,
            ingestion.TilesQueried,
            ingestion.FeaturesFetched,
            ingestion.FeaturesUpserted,
            ingestion.TilesFailed);

        var areasById = new Dictionary<string, StoredFeature>(StringComparer.Ordinal);
        foreach (var points in decodedRoutes)
        {
            foreach (var point in points)
            {
                var containing = await _protectedAreasRepository.FindContainingPoint(point, cancellationToken);
                foreach (var area in containing)
                    areasById[area.Id] = area;
            }
        }
        _logger.LogDebug("Found {Count} nearby protected areas", areasById.Count);
        return areasById.Values;
    }

    private async Task<IEnumerable<StoredFeatureSummary>> FetchAdminRegionsForActivity(
        List<Coordinate> activityPoints,
        CancellationToken cancellationToken)
    {
        if (activityPoints.Count == 0)
            return [];

        var coordinates = SampleCoordinatesForAdminBoundaryLookup(activityPoints)
            .DistinctBy(p => (p.Lat, p.Lng))
            .ToList();

        // Ensure the admin regions used by this worker are stored before the containment query.
        var ingestion = await _osmFeatureIngestion.EnsureAdminBoundariesAsync(
            coordinates, AdminLevelRegion, cancellationToken) ?? OsmIngestionResult.Empty;
        _logger.LogInformation(
            "OSM admin boundary ingestion for adminLevel={AdminLevel}: tilesQueried={TilesQueried}, fetched={Fetched}, upserted={Upserted}, failed={Failed}",
            AdminLevelRegion,
            ingestion.TilesQueried,
            ingestion.FeaturesFetched,
            ingestion.FeaturesUpserted,
            ingestion.TilesFailed);

        var summaries = (await _adminBoundariesRepository.FindSummariesContainingAnyPoint(
            coordinates, AdminLevelRegion, cancellationToken)).ToList();

        if (summaries.Count == 0)
        {
            _logger.LogDebug("Found 0 admin region candidates via ST_WITHIN");
            return [];
        }

        if (summaries.Count == 1)
        {
            _logger.LogDebug("Found a single admin region candidate via ST_WITHIN; skipping local geometry validation and full boundary fetch.");
            return summaries;
        }

        var boundaryDocsById = (await _adminBoundariesRepository.GetByIds(
                summaries.Select(summary => summary.Id),
                cancellationToken))
            .ToDictionary(doc => doc.Id, StringComparer.Ordinal);

        var validatedSummaries = ValidateAdminRegionCandidates(
            summaries,
            boundaryDocsById,
            activityPoints);

        if (validatedSummaries.Count == 0)
        {
            _logger.LogWarning(
                "All {CandidateCount} admin region candidates for this activity were rejected by local geometry validation.",
                summaries.Count);
            return [];
        }

        _logger.LogDebug(
            "Found {CandidateCount} admin region candidates via ST_WITHIN; {ValidatedCount} remained after local geometry validation",
            summaries.Count,
            validatedSummaries.Count);
        return validatedSummaries;
    }

    private static StoredFeatureSummary MergeSummaryWithBoundaryDoc(
        StoredFeatureSummary summary,
        StoredFeature boundaryDoc)
    {
        var mergedProperties = new Dictionary<string, dynamic>(summary.Properties, StringComparer.Ordinal);
        foreach (var (key, value) in boundaryDoc.Properties)
            mergedProperties[key] = value;

        return new StoredFeatureSummary
        {
            Id = summary.Id,
            FeatureId = summary.FeatureId,
            Kind = summary.Kind,
            Properties = mergedProperties
        };
    }

    private static bool ActivityIntersectsBoundary(
        IEnumerable<Coordinate> activityPoints,
        Geometry geometry)
    {
        foreach (var point in activityPoints)
        {
            if (RouteFeatureMatcher.IsPointInGeometry(point, geometry))
                return true;
        }

        return false;
    }

    private static IEnumerable<Coordinate> SampleCoordinatesForAdminBoundaryLookup(List<Coordinate> activityPoints)
    {
        if (activityPoints.Count <= MaxAdminBoundaryLookupPoints)
            return activityPoints;

        var step = (int)Math.Ceiling((double)activityPoints.Count / MaxAdminBoundaryLookupPoints);
        var sampled = activityPoints
            .Where((_, i) => i % step == 0)
            .ToList();

        // Ensure the end of the route is represented even when it does not align with the step.
        var last = activityPoints[^1];
        if (sampled.Count == 0 || sampled[^1] != last)
            sampled.Add(last);

        return sampled;
    }

    internal static List<StoredFeatureSummary> ValidateAdminRegionCandidates(
        IEnumerable<StoredFeatureSummary> summaries,
        IReadOnlyDictionary<string, StoredFeature> boundaryDocsById,
        IEnumerable<Coordinate> activityPoints)
    {
        return summaries
            .Select(summary => boundaryDocsById.TryGetValue(summary.Id, out var boundaryDoc)
                && ActivityIntersectsBoundary(activityPoints, boundaryDoc.Geometry)
                    ? MergeSummaryWithBoundaryDoc(summary, boundaryDoc)
                    : null)
            .Where(summary => summary is not null)
            .Select(summary => summary!)
            .ToList();
    }

    internal static string? GetRoutePolyline(Activity activity)
    {
        if (!string.IsNullOrWhiteSpace(activity.Polyline))
            return activity.Polyline;

        if (!string.IsNullOrWhiteSpace(activity.SummaryPolyline))
            return activity.SummaryPolyline;

        return null;
    }

    internal static string? GetBoundaryLookupPolyline(Activity activity)
    {
        return GetRoutePolyline(activity);
    }

}
