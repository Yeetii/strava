using System.Globalization;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.DependencyInjection;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public sealed class AdminBoundaryMetricsEnricher(
    [FromKeyedServices(FeatureKinds.Peak)] TiledCollectionClient peaksCollection,
    [FromKeyedServices(FeatureKinds.ProtectedArea)] TiledCollectionClient protectedAreasCollection)
{
    public const int AnalysisTileZoom = 8;
    public const int BorderSamplingStep = 10;
    public const int MetricsVersion = 1;

    private readonly TiledCollectionClient _peaksCollection = peaksCollection;
    private readonly TiledCollectionClient _protectedAreasCollection = protectedAreasCollection;

    public async Task EnrichAsync(StoredFeature boundary, CancellationToken cancellationToken = default)
    {
        var candidateTiles = CalculateCandidateTiles(boundary.Geometry, AnalysisTileZoom, BorderSamplingStep);
        var peaks = await _peaksCollection.FetchByTiles(candidateTiles, AnalysisTileZoom, cancellationToken);
        var protectedAreas = await _protectedAreasCollection.FetchByTiles(candidateTiles, AnalysisTileZoom, cancellationToken);

        var metrics = CalculateMetrics(boundary, peaks, protectedAreas);
        foreach (var (key, value) in metrics)
        {
            boundary.Properties[key] = value;
        }
    }

    public static HashSet<(int x, int y)> CalculateCandidateTiles(Geometry geometry, int zoom = AnalysisTileZoom, int borderSamplingStep = BorderSamplingStep)
    {
        var candidateTiles = new HashSet<(int x, int y)>();
        var bounds = GetBounds(geometry);

        foreach (var ring in GetOuterRings(geometry))
        {
            foreach (var coordinate in SampleRing(ring, borderSamplingStep))
            {
                candidateTiles.Add(SlippyTileCalculator.WGS84ToTileIndex(coordinate, zoom));
            }
        }

        var northWest = new Coordinate(bounds.min.Lng, bounds.max.Lat);
        var southEast = new Coordinate(bounds.max.Lng, bounds.min.Lat);
        var (minX, minY) = SlippyTileCalculator.WGS84ToTileIndex(northWest, zoom);
        var (maxX, maxY) = SlippyTileCalculator.WGS84ToTileIndex(southEast, zoom);

        for (var x = minX; x <= maxX; x++)
        {
            for (var y = minY; y <= maxY; y++)
            {
                var center = GetTileCenter(x, y, zoom);
                if (RouteFeatureMatcher.IsPointInGeometry(center, geometry))
                {
                    candidateTiles.Add((x, y));
                }
            }
        }

        return candidateTiles;
    }

    public static IDictionary<string, object?> CalculateMetrics(
        StoredFeature boundary,
        IEnumerable<StoredFeature> peaks,
        IEnumerable<StoredFeature> protectedAreas)
    {
        var peaksWithin = peaks
            .Where(peak => peak.Kind == FeatureKinds.Peak)
            .Where(peak => RouteFeatureMatcher.IsPointInGeometry(GeometryCentroidHelper.GetCentroid(peak.Geometry), boundary.Geometry))
            .ToList();

        var tallestPeak = peaksWithin
            .Select(peak => new { Peak = peak, Elevation = GetElevation(peak) })
            .OrderByDescending(item => item.Elevation ?? double.MinValue)
            .FirstOrDefault(item => item.Elevation.HasValue)
            ?.Peak;

        var protectedAreasWithin = protectedAreas
            .Where(area => area.Kind == FeatureKinds.ProtectedArea)
            .Where(area => RouteFeatureMatcher.IsPointInGeometry(GeometryCentroidHelper.GetCentroid(area.Geometry), boundary.Geometry))
            .ToList();

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

    private static int CountProtectedAreasOfType(IEnumerable<StoredFeature> protectedAreas, string areaType)
    {
        return protectedAreas.Count(area => string.Equals(GetPropertyValue(area, "areaType"), areaType, StringComparison.Ordinal));
    }

    private static Dictionary<string, object?> CreatePeakSummary(StoredFeature peak)
    {
        var centroid = GeometryCentroidHelper.GetCentroid(peak.Geometry);
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

    private static double? GetElevation(StoredFeature peak)
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

    private static string? GetPropertyValue(StoredFeature feature, string key)
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