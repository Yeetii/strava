using System.Diagnostics;
using System.Globalization;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public sealed class AdminBoundaryMetricsEnricher(
    [FromKeyedServices(FeatureKinds.Peak)] TiledCollectionClient peaksCollection,
    [FromKeyedServices(FeatureKinds.ProtectedArea)] TiledCollectionClient protectedAreasCollection,
    AdminBoundariesCollectionClient adminBoundariesCollection,
    ILogger<AdminBoundaryMetricsEnricher> logger)
{
    public const int AnalysisTileZoom = 8;
    public const int BorderSamplingStep = 10;
    public const int MetricsVersion = 2;
    /// <summary>Storage zoom for the peaks collection (must match <see cref="CollectionClientBuilder"/>).</summary>
    public const int PeakStorageZoom = 11;
    /// <summary>
    /// Maximum number of zoom-8 border tiles (tiles the boundary ring passes through).
    /// Border tiles require a per-peak <c>IsPointInGeometry</c> check and drive the expensive work.
    /// Interior tiles are cheap: all their z11 sub-tiles are guaranteed inside, so peaks require
    /// no geometry check. Boundaries with too many border tiles only receive a version stamp.
    /// </summary>
    public const int MaxBorderZ8Tiles = 200;

    private readonly TiledCollectionClient _peaksCollection = peaksCollection;
    private readonly TiledCollectionClient _protectedAreasCollection = protectedAreasCollection;
    private readonly AdminBoundariesCollectionClient _adminBoundariesCollection = adminBoundariesCollection;

    public async Task<IReadOnlyList<PatchOperation>> CalculatePatchOperationsAsync(StoredFeature boundary, CancellationToken cancellationToken = default)
    {
        // Country-level boundaries (adminLevel=2) are too large to enrich meaningfully.
        if (IsAdminLevel(boundary, 2))
        {
            logger.LogInformation("[AdminBoundaryMetricsEnricher] Skip {BoundaryId}: country-level boundary", boundary.Id);
            return [PatchOperation.Set("/properties/adminBoundaryMetricsVersion", MetricsVersion)];
        }

        // --- Tile classification (pure CPU / geo) ---
        var (interiorZ8, borderZ8) = CalculateCandidateTilesSplit(boundary.Geometry, AnalysisTileZoom, BorderSamplingStep);

        // Too many border tiles → boundary is very large or complex; stamp the version and move on.
        if (borderZ8.Count > MaxBorderZ8Tiles)
        {
            logger.LogInformation("[AdminBoundaryMetricsEnricher] {BoundaryId}: {BrdZ8} border z8 tiles exceeds limit {Limit}; stamping version only", boundary.Id, borderZ8.Count, MaxBorderZ8Tiles);
            var earlyOps = new List<PatchOperation> { PatchOperation.Set("/properties/adminBoundaryMetricsVersion", MetricsVersion) };
            var derivedCode = TryExtractCountryCodeFromProperties(boundary.Properties);
            if (derivedCode != null)
                earlyOps.Add(PatchOperation.Set("/properties/countryCode", derivedCode));
            return earlyOps;
        }

        logger.LogInformation("[AdminBoundaryMetricsEnricher] Start {BoundaryId} z8[int={IntZ8} brd={BrdZ8}]",
            boundary.Id, interiorZ8.Count, borderZ8.Count);

        var peakIntResult = interiorZ8.Count > 0
            ? await _peaksCollection.FetchSlimByTilesMeasured(interiorZ8, AnalysisTileZoom, populateMissingTiles: false, cancellationToken: cancellationToken)
            : new CollectionClient<StoredFeature>.MeasuredResult<IEnumerable<StoredFeatureSummary>>([], 0);
        var interiorPeaks = peakIntResult.Value.Where(p => p.Kind == FeatureKinds.Peak).ToList();

        // --- Border peaks (centroid check per peak) ---
        var peakBrdResult = borderZ8.Count > 0
            ? await _peaksCollection.FetchSlimByTilesMeasured(borderZ8, AnalysisTileZoom, populateMissingTiles: false, cancellationToken: cancellationToken)
            : new CollectionClient<StoredFeature>.MeasuredResult<IEnumerable<StoredFeatureSummary>>([], 0);
        var borderPeakCandidates = peakBrdResult.Value.Where(p => p.Kind == FeatureKinds.Peak).ToList();
        var borderPeaks = borderPeakCandidates
            .Where(p => RouteFeatureMatcher.IsPointInGeometry(p.ResolvedCentroid, boundary.Geometry))
            .ToList();
        var peaksWithin = (IReadOnlyList<StoredFeatureSummary>)interiorPeaks.Concat(borderPeaks).ToList();

        // --- Interior protected areas (no geometry check needed) ---
        var paIntResult = interiorZ8.Count > 0
            ? await _protectedAreasCollection.FetchSlimByTilesMeasured(interiorZ8, AnalysisTileZoom, followPointers: true, populateMissingTiles: false, cancellationToken: cancellationToken)
            : new CollectionClient<StoredFeature>.MeasuredResult<IEnumerable<StoredFeatureSummary>>([], 0);
        var interiorProtectedAreas = paIntResult.Value.Where(a => a.Kind == FeatureKinds.ProtectedArea).ToList();

        // --- Border protected areas (centroid check per area) ---
        var paBrdResult = borderZ8.Count > 0
            ? await _protectedAreasCollection.FetchSlimByTilesMeasured(borderZ8, AnalysisTileZoom, followPointers: true, populateMissingTiles: false, cancellationToken: cancellationToken)
            : new CollectionClient<StoredFeature>.MeasuredResult<IEnumerable<StoredFeatureSummary>>([], 0);
        var borderPACandidates = paBrdResult.Value.Where(a => a.Kind == FeatureKinds.ProtectedArea).ToList();
        var borderProtectedAreas = borderPACandidates
            .Where(a => RouteFeatureMatcher.IsPointInGeometry(a.ResolvedCentroid, boundary.Geometry))
            .ToList();

        var protectedAreasWithin = (IReadOnlyList<StoredFeatureSummary>)interiorProtectedAreas.Concat(borderProtectedAreas).ToList();

        var metrics = BuildMetricsDictionary(peaksWithin, protectedAreasWithin);
        var ops = new List<PatchOperation>();
        foreach (var (key, value) in metrics)
            ops.Add(PatchOperation.Set($"/properties/{key}", value));

        ops.AddRange(await GetCountryPatchOpsAsync(boundary, cancellationToken));

        logger.LogInformation(
            "[AdminBoundaryMetricsEnricher] Done {BoundaryId}: {OpCount} ops | peaks={Peaks} PA={PA} | RU={RU:F1}",
            boundary.Id, ops.Count,
            peaksWithin.Count, protectedAreasWithin.Count,
            peakIntResult.RequestCharge + peakBrdResult.RequestCharge + paIntResult.RequestCharge + paBrdResult.RequestCharge);
        return ops;
    }

    private async Task<IReadOnlyList<PatchOperation>> GetCountryPatchOpsAsync(StoredFeature boundary, CancellationToken cancellationToken)
    {
        var ops = new List<PatchOperation>();
        var centroid = boundary.ResolvedCentroid;

        var country = (await _adminBoundariesCollection.FindBoundarySummariesContainingAnyPoint(
            [centroid], adminLevel: 2, cancellationToken: cancellationToken)).FirstOrDefault();

        if (country != null)
        {
            ops.Add(PatchOperation.Set("/properties/countryId", country.Id));
            if (country.Properties.TryGetValue("countryCode", out var code)
                && code?.ToString() is string countryCode)
            {
                ops.Add(PatchOperation.Set("/properties/countryCode", countryCode));
                return ops;
            }
        }

        // Fall back: derive country code from the boundary's own OSM tags.
        var derivedCode = TryExtractCountryCodeFromProperties(boundary.Properties);
        if (derivedCode != null)
            ops.Add(PatchOperation.Set("/properties/countryCode", derivedCode));

        return ops;
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
            ["adminBoundaryMetricsVersion"] = MetricsVersion,
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