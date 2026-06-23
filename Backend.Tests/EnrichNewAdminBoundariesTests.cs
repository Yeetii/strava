using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Backend.Tests;

public class EnrichNewAdminBoundariesTests
{
    [Fact]
    public void ShouldEnrich_ReturnsFalse_WhenMetricsVersionIsCurrent()
    {
        var document = CreateBoundary();
        document.Properties["adminBoundaryMetricsVersion"] = AdminBoundaryMetricsEnricher.MetricsVersion;

        var shouldEnrich = OsmFeaturesChangeTrigger.ShouldEnrich(document);

        Assert.False(shouldEnrich);
    }

    [Fact]
    public void ShouldEnrich_ReturnsTrue_WhenMetricsVersionIsOutdated()
    {
        var document = CreateBoundary();
        document.Properties["adminBoundaryMetricsVersion"] = AdminBoundaryMetricsEnricher.MetricsVersion - 1;

        var shouldEnrich = OsmFeaturesChangeTrigger.ShouldEnrich(document);

        Assert.True(shouldEnrich);
    }

    [Fact]
    public void ShouldEnrich_ReturnsFalse_ForPointerDocument()
    {
        var document = StoredFeature.CreatePointer(
            FeatureKinds.AdminBoundary,
            "boundary-1",
            x: 1,
            y: 2,
            zoom: 6,
            storedX: 3,
            storedY: 4,
            storedZoom: 6,
            storedDocumentId: "adminBoundary:4:boundary-1");

        var shouldEnrich = OsmFeaturesChangeTrigger.ShouldEnrich(document);

        Assert.False(shouldEnrich);
    }

    private static StoredFeature CreateBoundary()
    {
        return new StoredFeature
        {
            Id = "adminBoundary:4:boundary-1",
            FeatureId = "boundary-1",
            Kind = FeatureKinds.AdminBoundary,
            X = 0,
            Y = 0,
            Zoom = 6,
            Geometry = new Point(new Position(0, 0))
        };
    }
}
