using System.Globalization;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Stigar.Core.Geo;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Core.Services;

/// <summary>
/// Calculates admin-boundary metrics from the PostGIS domain tables. The output shape (property
/// names and values) and the <c>adminBoundaryMetricsVersion</c> stamp are unchanged from the
/// Cosmos implementation; only the reads and the persistence handoff changed.
/// </summary>
public sealed class AdminBoundaryMetricsEnricher(
    IPeakRepository peaks,
    IAdminBoundaryRepository adminBoundaries,
    IBackendAdminQueries queries,
    ILogger<AdminBoundaryMetricsEnricher> logger)
{
    public const int AnalysisTileZoom = 8;
    public const int BorderSamplingStep = 10;
    /// <summary>Storage zoom for the peaks collection.</summary>
    public const int PeakStorageZoom = 11;
    /// <summary>
    /// Maximum number of zoom-8 border tiles (tiles the boundary ring passes through).
    /// Border tiles require a per-peak <c>IsPointInGeometry</c> check and drive the expensive work.
    /// Interior tiles are cheap: all their z11 sub-tiles are guaranteed inside, so peaks require
    /// no geometry check. Boundaries with too many border tiles only receive a version stamp.
    /// </summary>
    public const int MaxBorderZ8Tiles = 200;

    /// <summary>
    /// Computes the metrics to merge into an admin boundary's JSONB properties. The dictionary
    /// keys are the same property names the old Cosmos patches set (minus the "/properties/"
    /// prefix), so the enriched document shape is unchanged.
    /// </summary>
    public async Task<IReadOnlyDictionary<string, object?>> CalculateMetricsAsync(
        StoredFeature boundary,
        CancellationToken cancellationToken = default)
    {
        // Country-level boundaries (adminLevel=2) are too large to enrich meaningfully.
        if (IsAdminLevel(boundary, 2))
        {
            logger.LogInformation("[AdminBoundaryMetricsEnricher] Skip {BoundaryId}: country-level boundary", boundary.Id);
            return new Dictionary<string, object?>();
        }

        var centroid = boundary.ResolvedCentroid;

        // PostGIS does the spatial prefilter directly. A radius covering the boundary's bounding
        // box is a superset of the peaks we want; the exact geometry test below narrows it to the
        // same "peak centroid inside boundary" set the tile-based Cosmos implementation produced.
        var (min, max) = GetBounds(boundary.Geometry);
        var radiusMeters = RadiusCoveringBounds(centroid, min, max);
        var peakCandidates = await peaks.FindWithinRadius(centroid, radiusMeters, cancellationToken);
        var peaksWithin = (IReadOnlyList<StoredFeatureSummary>)peakCandidates
            .Select(StoredFeatureSummary.FromStoredFeature)
            .Where(p => RouteFeatureMatcher.IsPointInGeometry(p.ResolvedCentroid, boundary.Geometry))
            .ToList();

        // Protected areas: centroid-within-boundary, evaluated with ST_Intersects in the query layer.
        var protectedAreasWithin = await queries.FindProtectedAreaSummariesWithinBoundary(
            boundary.Geometry, cancellationToken);

        var metrics = BuildMetricsDictionary(peaksWithin, protectedAreasWithin);
        await AddCountryMetricsAsync(metrics, boundary, cancellationToken);

        logger.LogInformation(
            "[AdminBoundaryMetricsEnricher] Done {BoundaryId}: peaks={Peaks} PA={PA}",
            boundary.Id, peaksWithin.Count, protectedAreasWithin.Count);
        return new Dictionary<string, object?>(metrics);
    }

    /// <summary>Haversine distance from the centroid to the farthest bounding-box corner, in metres.</summary>
    private static int RadiusCoveringBounds(Coordinate centre, Coordinate min, Coordinate max)
    {
        const double earthRadiusMetres = 6_371_000;

        double DistanceTo(Coordinate corner)
        {
            var lat1 = centre.Lat * Math.PI / 180.0;
            var lat2 = corner.Lat * Math.PI / 180.0;
            var dLat = lat2 - lat1;
            var dLng = (corner.Lng - centre.Lng) * Math.PI / 180.0;
            var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2)
                    + Math.Cos(lat1) * Math.Cos(lat2) * Math.Sin(dLng / 2) * Math.Sin(dLng / 2);
            return 2 * earthRadiusMetres * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
        }

        var farthest = new[]
        {
            DistanceTo(new Coordinate(min.Lng, min.Lat)),
            DistanceTo(new Coordinate(min.Lng, max.Lat)),
            DistanceTo(new Coordinate(max.Lng, min.Lat)),
            DistanceTo(new Coordinate(max.Lng, max.Lat)),
        }.Max();

        return (int)Math.Min(int.MaxValue, Math.Ceiling(farthest) + 1_000);
    }

    private async Task AddCountryMetricsAsync(
        IDictionary<string, object?> metrics,
        StoredFeature boundary,
        CancellationToken cancellationToken)
    {
        var country = (await adminBoundaries.FindSummariesContainingAnyPoint(
            [boundary.ResolvedCentroid], adminLevel: 2, cancellationToken: cancellationToken)).FirstOrDefault();

        if (country != null)
        {
            metrics["countryId"] = country.Id;
            if (country.Properties.TryGetValue("countryCode", out var code)
                && code?.ToString() is string countryCode)
            {
                metrics["countryCode"] = countryCode;
                return;
            }
        }

        // Fall back: derive country code from the boundary's own OSM tags.
        var derivedCode = TryExtractCountryCodeFromProperties(boundary.Properties);
        if (derivedCode != null)
            metrics["countryCode"] = derivedCode;
    }

    /// <summary>
    /// Attempts to derive a 2-letter ISO 3166-1 country code from OSM properties
    /// without requiring a country-boundary lookup.
    /// <list type="bullet">
    ///   <item><c>ISO3166-2</c> — splits at <c>-</c> and returns the first segment (e.g. "SE-C" → "SE").</item>
    ///   <item><c>ref:nuts</c> — returns the first 2 characters (e.g. "SE011" → "SE").</item>
    /// </list>
    /// </summary>
    internal static string? TryExtractCountryCodeFromProperties(IDictionary<string, dynamic> props)
    {
        var isoValue = props
            .FirstOrDefault(kv => string.Equals(kv.Key, "ISO3166-2", StringComparison.OrdinalIgnoreCase))
            .Value?.ToString() as string;
        if (isoValue != null)
        {
            var parts = isoValue.Split('-');
            if (parts.Length >= 2 && parts[0].Length == 2)
                return parts[0].ToUpperInvariant();
        }

        var nutsValue = props
            .FirstOrDefault(kv => string.Equals(kv.Key, "ref:nuts", StringComparison.OrdinalIgnoreCase))
            .Value?.ToString() as string;
        if (nutsValue != null && nutsValue.Length >= 2)
            return nutsValue.Substring(0, 2).ToUpperInvariant();

        return null;
    }

    private static bool IsAdminLevel(StoredFeature boundary, int level)
    {
        return boundary.Properties.TryGetValue("adminLevel", out var val)
            && string.Equals(val?.ToString(), level.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Returns all zoom-<paramref name="zoom"/> tiles that intersect the boundary.
    /// This is a convenience wrapper over <see cref="CalculateCandidateTilesSplit"/>.
    /// </summary>
    public static HashSet<(int x, int y)> CalculateCandidateTiles(Geometry geometry, int zoom = AnalysisTileZoom, int borderSamplingStep = BorderSamplingStep)
    {
        var (interior, border) = CalculateCandidateTilesSplit(geometry, zoom, borderSamplingStep);
        interior.UnionWith(border);
        return interior;
    }

    /// <summary>
    /// Splits candidate tiles into two sets:
    /// <list type="bullet">
    /// <item><description><b>Border</b> — tiles the boundary ring passes through (sampled every
    /// <paramref name="borderSamplingStep"/> vertices). These tiles straddle the boundary edge
    /// and require a per-feature geometry check.</description></item>
    /// <item><description><b>Interior</b> — tiles whose centre is inside the boundary but that
    /// the sampled ring never touches. All features in these tiles are inside the boundary;
    /// no geometry check is needed.</description></item>
    /// </list>
    /// </summary>
    public static (HashSet<(int x, int y)> Interior, HashSet<(int x, int y)> Border) CalculateCandidateTilesSplit(
        Geometry geometry, int zoom = AnalysisTileZoom, int borderSamplingStep = BorderSamplingStep)
    {
        var borderTiles = new HashSet<(int x, int y)>();
        var (min, max) = GetBounds(geometry);

        foreach (var ring in GetOuterRings(geometry))
        {
            foreach (var coordinate in SampleRing(ring, borderSamplingStep))
            {
                borderTiles.Add(SlippyTileCalculator.WGS84ToTileIndex(coordinate, zoom));
            }
        }

        var interiorTiles = new HashSet<(int x, int y)>();
        var northWest = new Coordinate(min.Lng, max.Lat);
        var southEast = new Coordinate(max.Lng, min.Lat);
        var (minX, minY) = SlippyTileCalculator.WGS84ToTileIndex(northWest, zoom);
        var (maxX, maxY) = SlippyTileCalculator.WGS84ToTileIndex(southEast, zoom);

        for (var x = minX; x <= maxX; x++)
        {
            for (var y = minY; y <= maxY; y++)
            {
                if (borderTiles.Contains((x, y)))
                    continue;
                var center = GetTileCenter(x, y, zoom);
                if (RouteFeatureMatcher.IsPointInGeometry(center, geometry))
                    interiorTiles.Add((x, y));
            }
        }

        return (interiorTiles, borderTiles);
    }

    /// <summary>
    /// Expands a single tile at <paramref name="fromZoom"/> into all sub-tiles at
    /// <paramref name="toZoom"/> (must be ≥ fromZoom).
    /// </summary>
    private static IEnumerable<(int x, int y)> ExpandTileToZoom((int x, int y) tile, int fromZoom, int toZoom)
    {
        var scale = 1 << (toZoom - fromZoom);
        var baseX = tile.x * scale;
        var baseY = tile.y * scale;
        for (var dx = 0; dx < scale; dx++)
            for (var dy = 0; dy < scale; dy++)
                yield return (baseX + dx, baseY + dy);
    }

    /// <summary>
    /// Returns <c>true</c> when all four corners of the tile are inside <paramref name="boundary"/>,
    /// meaning all features whose geometry falls within this tile are also inside the boundary.
    /// </summary>
    private static bool IsTileFullyInside((int x, int y) tile, int zoom, Geometry boundary)
    {
        var (sw, ne) = SlippyTileCalculator.TileIndexToWGS84(tile.x, tile.y, zoom);
        return RouteFeatureMatcher.IsPointInGeometry(new Coordinate(sw.Lng, sw.Lat), boundary)
            && RouteFeatureMatcher.IsPointInGeometry(new Coordinate(ne.Lng, ne.Lat), boundary)
            && RouteFeatureMatcher.IsPointInGeometry(new Coordinate(sw.Lng, ne.Lat), boundary)
            && RouteFeatureMatcher.IsPointInGeometry(new Coordinate(ne.Lng, sw.Lat), boundary);
    }

    /// <summary>
    /// Calculates metrics by applying a geometry check to all supplied peaks and protected areas.
    /// Used by tests and the backfill function which don't do the interior/border tile split.
    /// </summary>
    public static IDictionary<string, object?> CalculateMetrics(
        StoredFeature boundary,
        IEnumerable<StoredFeatureSummary> peaks,
        IEnumerable<StoredFeatureSummary> protectedAreas)
    {
        var peaksWithin = peaks
            .Where(peak => peak.Kind == FeatureKinds.Peak)
            .Where(peak => RouteFeatureMatcher.IsPointInGeometry(peak.ResolvedCentroid, boundary.Geometry))
            .ToList();

        var protectedAreasFiltered = protectedAreas
            .Where(area => area.Kind == FeatureKinds.ProtectedArea)
            .Where(area => RouteFeatureMatcher.IsPointInGeometry(area.ResolvedCentroid, boundary.Geometry))
            .ToList();

        return BuildMetricsDictionary(peaksWithin, protectedAreasFiltered);
    }

    /// <summary>
    /// Builds the metrics dictionary from pre-filtered peaks and pre-filtered protected areas.
    /// Neither collection is geo-checked inside this method.
    /// </summary>
    private static IDictionary<string, object?> BuildMetricsDictionary(
        IReadOnlyList<StoredFeatureSummary> peaksWithin,
        IReadOnlyList<StoredFeatureSummary> protectedAreasWithin)
    {
        var tallestPeak = peaksWithin
            .Select(peak => new { Peak = peak, Elevation = GetElevation(peak) })
            .OrderByDescending(item => item.Elevation ?? double.MinValue)
            .FirstOrDefault(item => item.Elevation.HasValue)
            ?.Peak;

        return new Dictionary<string, object?>
        {
            ["totalPeaksWithin"] = peaksWithin.Count,
            ["tallestPeakWithin"] = tallestPeak == null ? null : CreatePeakSummary(tallestPeak),
            ["protectedAreaCounts"] = new Dictionary<string, int>
            {
                ["nationalParks"] = CountProtectedAreasOfType(protectedAreasWithin, "national_park"),
                ["natureReserves"] = CountProtectedAreasOfType(protectedAreasWithin, "nature_reserve"),
                ["protectedAreas"] = CountProtectedAreasOfType(protectedAreasWithin, "protected_area")
            }
        };
    }

    private static int CountProtectedAreasOfType(IEnumerable<StoredFeatureSummary> protectedAreas, string areaType)
    {
        return protectedAreas.Count(area => string.Equals(GetPropertyValue(area, "areaType"), areaType, StringComparison.Ordinal));
    }

    private static Dictionary<string, object?> CreatePeakSummary(StoredFeatureSummary peak)
    {
        var centroid = peak.ResolvedCentroid;
        return new Dictionary<string, object?>
        {
            ["id"] = peak.Id,
            ["featureId"] = peak.LogicalId,
            ["name"] = GetPropertyValue(peak, "name"),
            ["elevation"] = GetElevation(peak),
            ["lat"] = centroid.Lat,
            ["lng"] = centroid.Lng
        };
    }

    private static double? GetElevation(StoredFeatureSummary peak)
    {
        var rawElevation = GetPropertyValue(peak, "ele");
        if (string.IsNullOrWhiteSpace(rawElevation))
        {
            return null;
        }

        var normalized = rawElevation
            .Replace("m", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Trim();

        if (double.TryParse(normalized, NumberStyles.Float, CultureInfo.InvariantCulture, out var elevation))
        {
            return elevation;
        }

        return double.TryParse(normalized, NumberStyles.Float, CultureInfo.GetCultureInfo("sv-SE"), out elevation)
            ? elevation
            : null;
    }

    private static string? GetPropertyValue(StoredFeatureSummary feature, string key)
    {
        return feature.Properties.TryGetValue(key, out var value)
            ? value?.ToString()
            : null;
    }

    private static Coordinate GetTileCenter(int x, int y, int zoom)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        return new Coordinate(
            (southWest.Lng + northEast.Lng) / 2,
            (southWest.Lat + northEast.Lat) / 2);
    }

    private static IEnumerable<Coordinate> SampleRing(IReadOnlyList<Coordinate> ring, int borderSamplingStep)
    {
        if (ring.Count == 0)
        {
            yield break;
        }

        var step = Math.Max(1, borderSamplingStep);
        for (var index = 0; index < ring.Count; index += step)
        {
            yield return ring[index];
        }

        yield return ring[^1];
    }

    private static (Coordinate min, Coordinate max) GetBounds(Geometry geometry)
    {
        var coordinates = GetOuterRings(geometry)
            .SelectMany(ring => ring)
            .ToList();

        return (
            new Coordinate(coordinates.Min(coordinate => coordinate.Lng), coordinates.Min(coordinate => coordinate.Lat)),
            new Coordinate(coordinates.Max(coordinate => coordinate.Lng), coordinates.Max(coordinate => coordinate.Lat)));
    }

    private static IEnumerable<List<Coordinate>> GetOuterRings(Geometry geometry)
    {
        switch (geometry)
        {
            case Polygon polygon:
            {
                var outerRing = polygon.Coordinates.FirstOrDefault();
                if (outerRing != null)
                {
                    yield return outerRing.Coordinates.Select(position => new Coordinate(position.Longitude, position.Latitude)).ToList();
                }

                break;
            }
            case MultiPolygon multiPolygon:
            {
                foreach (var polygon in multiPolygon.Coordinates)
                {
                    var outerRing = polygon.Coordinates.FirstOrDefault();
                    if (outerRing != null)
                    {
                        yield return outerRing.Coordinates.Select(position => new Coordinate(position.Longitude, position.Latitude)).ToList();
                    }
                }

                break;
            }
            default:
                throw new NotSupportedException($"Geometry type {geometry.Type} is not supported for admin boundary enrichment.");
        }
    }
}