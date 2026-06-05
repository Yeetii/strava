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
            CreatePeakSummary("inside-low", 0, 0, "1200", "Lower Peak"),
            CreatePeakSummary("inside-high", 1, 1, "1800", "Higher Peak"),
            CreatePeakSummary("outside", 20, 20, "2500", "Outside Peak")
        };
        var protectedAreas = new[]
        {
            CreateProtectedAreaSummary("national-park", -1, -1, "national_park"),
            CreateProtectedAreaSummary("nature-reserve", 2, 2, "nature_reserve"),
            CreateProtectedAreaSummary("protected-area", -2, 2, "protected_area"),
            CreateProtectedAreaSummary("outside-area", 30, 30, "national_park")
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

    [Fact]
    public void CalculateCandidateTilesSplit_InteriorAndBorderSetsAreDisjoint()
    {
        var boundary = CreateBoundary(minLng: -20, minLat: -20, maxLng: 20, maxLat: 20);
        var (interior, border) = AdminBoundaryMetricsEnricher.CalculateCandidateTilesSplit(boundary.Geometry, zoom: 5, borderSamplingStep: 5);

        Assert.Empty(interior.Intersect(border));
    }

    [Fact]
    public void CalculateCandidateTilesSplit_UnionEqualsCalculateCandidateTiles()
    {
        var boundary = CreateBoundary(minLng: -20, minLat: -20, maxLng: 20, maxLat: 20);
        var geometry = boundary.Geometry;

        var (interior, border) = AdminBoundaryMetricsEnricher.CalculateCandidateTilesSplit(geometry, zoom: 5, borderSamplingStep: 5);
        var union = interior.Union(border).OrderBy(t => t.x).ThenBy(t => t.y).ToList();

        var legacy = AdminBoundaryMetricsEnricher.CalculateCandidateTiles(geometry, zoom: 5, borderSamplingStep: 5)
            .OrderBy(t => t.x).ThenBy(t => t.y).ToList();

        Assert.Equal(legacy, union);
    }

    [Fact]
    public void CalculateCandidateTilesSplit_InteriorTilesDoNotContainBorderRingCoordinates()
    {
        // A wide boundary so there are clearly interior tiles at zoom 4.
        var boundary = CreateBoundary(minLng: -40, minLat: -40, maxLng: 40, maxLat: 40);
        var (interior, border) = AdminBoundaryMetricsEnricher.CalculateCandidateTilesSplit(boundary.Geometry, zoom: 4, borderSamplingStep: 1);

        // The centre tile (0°, 0°) must land in the interior set, not border.
        var centerTile = Shared.Geo.SlippyTileCalculator.WGS84ToTileIndex(new Coordinate(0, 0), 4);
        Assert.Contains(centerTile, interior);
        Assert.DoesNotContain(centerTile, border);
    }

    [Fact]
    public void CalculateCandidateTilesSplit_SmallBoundaryProducesNonEmptyBorderSet()
    {
        var boundary = CreateBoundary(minLng: -5, minLat: -5, maxLng: 5, maxLat: 5);
        var (_, border) = AdminBoundaryMetricsEnricher.CalculateCandidateTilesSplit(boundary.Geometry, zoom: 8, borderSamplingStep: 1);

        Assert.NotEmpty(border);
    }

    // ── TryExtractCountryCodeFromProperties ─────────────────────────────────

    [Theory]
    [InlineData("SE-C",   "SE")]
    [InlineData("NO-03",  "NO")]
    [InlineData("DE-BY",  "DE")]
    [InlineData("GB-ENG", "GB")]
    public void TryExtractCountryCode_Iso31662_ReturnsFirstSegment(string value, string expected)
    {
        var props = new Dictionary<string, dynamic> { ["ISO3166-2"] = value };
        Assert.Equal(expected, AdminBoundaryMetricsEnricher.TryExtractCountryCodeFromProperties(props));
    }

    [Fact]
    public void TryExtractCountryCode_Iso31662_IsCaseInsensitiveKey()
    {
        // OSM often stores this as "isO3166-2"
        var props = new Dictionary<string, dynamic> { ["isO3166-2"] = "SE-C" };
        Assert.Equal("SE", AdminBoundaryMetricsEnricher.TryExtractCountryCodeFromProperties(props));
    }

    [Theory]
    [InlineData("SE011", "SE")]
    [InlineData("NO020", "NO")]
    [InlineData("DEF00", "DE")]
    public void TryExtractCountryCode_RefNuts_ReturnsFirstTwoChars(string value, string expected)
    {
        var props = new Dictionary<string, dynamic> { ["ref:nuts"] = value };
        Assert.Equal(expected, AdminBoundaryMetricsEnricher.TryExtractCountryCodeFromProperties(props));
    }

    [Fact]
    public void TryExtractCountryCode_Iso31662_TakesPriorityOverRefNuts()
    {
        var props = new Dictionary<string, dynamic>
        {
            ["ISO3166-2"] = "SE-C",
            ["ref:nuts"] = "NO020"
        };
        Assert.Equal("SE", AdminBoundaryMetricsEnricher.TryExtractCountryCodeFromProperties(props));
    }

    [Fact]
    public void TryExtractCountryCode_NoRelevantProps_ReturnsNull()
    {
        var props = new Dictionary<string, dynamic> { ["name"] = "Some Region" };
        Assert.Null(AdminBoundaryMetricsEnricher.TryExtractCountryCodeFromProperties(props));
    }

    [Theory]
    [InlineData("SE")]    // no dash → only one segment
    [InlineData("X-AB")]  // first segment is 1 char, not 2
    public void TryExtractCountryCode_Iso31662_InvalidFormat_FallsThrough(string value)
    {
        var props = new Dictionary<string, dynamic> { ["ISO3166-2"] = value };
        Assert.Null(AdminBoundaryMetricsEnricher.TryExtractCountryCodeFromProperties(props));
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

    private static StoredFeatureSummary CreatePeakSummary(string featureId, double lng, double lat, string elevation, string name)
    {
        return new StoredFeatureSummary
        {
            Id = $"peak:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.Peak,
            X = 0,
            Y = 0,
            Zoom = 8,
            Centroid = new Coordinate(lng, lat),
            Properties = new Dictionary<string, dynamic>
            {
                ["ele"] = elevation,
                ["name"] = name
            }
        };
    }

    private static StoredFeatureSummary CreateProtectedAreaSummary(string featureId, double centerLng, double centerLat, string areaType)
    {
        return new StoredFeatureSummary
        {
            Id = $"protectedArea:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.ProtectedArea,
            X = 0,
            Y = 0,
            Zoom = 8,
            Centroid = new Coordinate(centerLng, centerLat),
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