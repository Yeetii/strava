using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class AdminBoundaryMetricsEnricherTests
{
    [Fact]
    public void CalculateMetrics_ReturnsPeakAndProtectedAreaSummaries()
    {
        var boundary = CreateBoundary();
        var peaks = new[]
        {
            CreatePeak("inside-low", 0, 0, "1200", "Lower Peak"),
            CreatePeak("inside-high", 1, 1, "1800", "Higher Peak"),
            CreatePeak("outside", 20, 20, "2500", "Outside Peak")
        };
        var protectedAreas = new[]
        {
            CreateProtectedArea("national-park", -1, -1, "national_park"),
            CreateProtectedArea("nature-reserve", 2, 2, "nature_reserve"),
            CreateProtectedArea("protected-area", -2, 2, "protected_area"),
            CreateProtectedArea("outside-area", 30, 30, "national_park")
        };

        var metrics = AdminBoundaryMetricsEnricher.CalculateMetrics(boundary, peaks, protectedAreas);

        Assert.Equal(2, metrics["totalPeaksWithin"]);

        var tallestPeak = Assert.IsType<Dictionary<string, object?>>(metrics["tallestPeakWithin"]);
        Assert.Equal("inside-high", tallestPeak["featureId"]);
        Assert.Equal("Higher Peak", tallestPeak["name"]);
        Assert.Equal(1800d, tallestPeak["elevation"]);

        var protectedAreaCounts = Assert.IsType<Dictionary<string, int>>(metrics["protectedAreaCounts"]);
        Assert.Equal(1, protectedAreaCounts["nationalParks"]);
        Assert.Equal(1, protectedAreaCounts["natureReserves"]);
        Assert.Equal(1, protectedAreaCounts["protectedAreas"]);
    }

    [Fact]
    public void CalculateCandidateTiles_IncludesInteriorTileCenters()
    {
        var boundary = CreateBoundary(minLng: -20, minLat: -20, maxLng: 20, maxLat: 20);

        var candidateTiles = AdminBoundaryMetricsEnricher.CalculateCandidateTiles(boundary.Geometry, zoom: 4, borderSamplingStep: 10);
        var centerTile = Shared.Geo.SlippyTileCalculator.WGS84ToTileIndex(new Coordinate(0, 0), 4);

        Assert.Contains(centerTile, candidateTiles);
    }

    private static StoredFeature CreateBoundary(double minLng = -5, double minLat = -5, double maxLng = 5, double maxLat = 5)
    {
        return new StoredFeature
        {
            Id = "adminBoundary:2:boundary-1",
            FeatureId = "boundary-1",
            Kind = FeatureKinds.AdminBoundary,
            X = 0,
            Y = 0,
            Zoom = 6,
            Geometry = CreateSquare(minLng, minLat, maxLng, maxLat)
        };
    }

    private static StoredFeature CreatePeak(string featureId, double lng, double lat, string elevation, string name)
    {
        return new StoredFeature
        {
            Id = $"peak:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.Peak,
            X = 0,
            Y = 0,
            Zoom = 8,
            Geometry = new Point(new Position(lng, lat)),
            Properties = new Dictionary<string, dynamic>
            {
                ["ele"] = elevation,
                ["name"] = name
            }
        };
    }

    private static StoredFeature CreateProtectedArea(string featureId, double centerLng, double centerLat, string areaType)
    {
        return new StoredFeature
        {
            Id = $"protectedArea:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.ProtectedArea,
            X = 0,
            Y = 0,
            Zoom = 8,
            Geometry = CreateSquare(centerLng - 0.5, centerLat - 0.5, centerLng + 0.5, centerLat + 0.5),
            Properties = new Dictionary<string, dynamic>
            {
                ["areaType"] = areaType
            }
        };
    }

    private static Polygon CreateSquare(double minLng, double minLat, double maxLng, double maxLat)
    {
        return new Polygon(
            [
                new LinearRing(
                    [
                        new Position(minLng, minLat),
                        new Position(maxLng, minLat),
                        new Position(maxLng, maxLat),
                        new Position(minLng, maxLat),
                        new Position(minLng, minLat)
                    ],
                    null)
            ],
            null);
    }
}