using BAMCIS.GeoJSON;
using Shared.Models;

namespace Backend.Tests;

public class VisitedAreasWorkerTests
{
    [Fact]
    public void GetBoundaryLookupPolyline_PrefersFullPolylineOverSummaryPolyline()
    {
        var activity = new Activity
        {
            Id = "activity-1",
            UserId = "user-1",
            Name = "Morning Run",
            SportType = SportTypes.RUN,
            StartDate = DateTime.UtcNow,
            StartDateLocal = DateTime.UtcNow,
            Polyline = "full-polyline",
            SummaryPolyline = "summary-polyline"
        };

        var polyline = VisitedAreasWorker.GetBoundaryLookupPolyline(activity);

        Assert.Equal("full-polyline", polyline);
    }

    [Fact]
    public void ValidateAdminRegionCandidates_ReturnsEmptyWhenNoBoundaryIntersectsActivity()
    {
        var summary = CreateSummary("boundary-1", "Summary name");
        var boundary = CreateBoundary("boundary-1", CreateSquare(1, 1, 2, 2));
        var activityPoints = new[] { new Coordinate(0, 0), new Coordinate(0.2, 0.2) };

        var validated = VisitedAreasWorker.ValidateAdminRegionCandidates(
            [summary],
            new Dictionary<string, StoredFeature> { [summary.Id] = boundary },
            activityPoints);

        Assert.Empty(validated);
    }

    [Fact]
    public void ValidateAdminRegionCandidates_MergesBoundaryPropertiesForIntersectingRegions()
    {
        var summary = CreateSummary("boundary-1", "Summary name");
        var boundary = CreateBoundary("boundary-1", CreateSquare(-1, -1, 1, 1));
        boundary.Properties["countryCode"] = "NO";
        boundary.Properties["name"] = "Boundary name";
        var activityPoints = new[] { new Coordinate(0, 0), new Coordinate(0.1, 0.1) };

        var validated = VisitedAreasWorker.ValidateAdminRegionCandidates(
            [summary],
            new Dictionary<string, StoredFeature> { [summary.Id] = boundary },
            activityPoints);

        var region = Assert.Single(validated);
        Assert.Equal(summary.Id, region.Id);
        Assert.Equal("Boundary name", region.Properties["name"]);
        Assert.Equal("NO", region.Properties["countryCode"]);
    }

    private static StoredFeatureSummary CreateSummary(string id, string name)
        => new()
        {
            Id = id,
            FeatureId = id,
            Kind = "adminBoundary",
            Properties = new Dictionary<string, dynamic>
            {
                ["name"] = name
            }
        };

    private static StoredFeature CreateBoundary(string id, Geometry geometry)
        => new()
        {
            Id = id,
            FeatureId = id,
            Kind = "adminBoundary",
            X = 0,
            Y = 0,
            Zoom = 6,
            Geometry = geometry,
            Centroid = new Coordinate(0, 0),
            Properties = new Dictionary<string, dynamic>()
        };

    private static Polygon CreateSquare(double minLng, double minLat, double maxLng, double maxLat)
        => new(
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
