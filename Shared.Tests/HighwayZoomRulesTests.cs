using BAMCIS.GeoJSON;
using Shared.Services.Shards;

namespace Shared.Tests;

public class HighwayZoomRulesTests
{
    [Fact]
    public void ShouldKeepFeature_KeepsOnlyMajorRoadsAtZoom7()
    {
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("primary"), 7));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("path"), 7));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("tertiary"), 7));
    }

    [Fact]
    public void ShouldKeepFeature_ShowsHighVisibilityTrailBackboneBeforeLowVisibility()
    {
        var highVisibilityPath = CreateFeature("path", trailVisibility: "good");
        var lowVisibilityPath = CreateFeature("path", trailVisibility: "bad");

        Assert.True(HighwayZoomRules.ShouldKeepFeature(highVisibilityPath, 8));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(lowVisibilityPath, 8));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(lowVisibilityPath, 10));
    }

    [Fact]
    public void ShouldKeepFeature_UsesSacScaleToDelayDemandingTrails()
    {
        var easyTrack = CreateFeature("track", sacScale: "hiking");
        var demandingTrack = CreateFeature("track", sacScale: "demanding_alpine_hiking");

        Assert.True(HighwayZoomRules.ShouldKeepFeature(easyTrack, 8));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(demandingTrack, 9));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(demandingTrack, 10));
    }

    [Fact]
    public void ShouldKeepFeature_DefersMinorRoadsUntilLaterZooms()
    {
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("tertiary"), 9));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("tertiary"), 10));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("service"), 11));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("service"), 12));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("residential"), 12));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("residential"), 13));
    }

    [Fact]
    public void ShouldKeepFeature_AppliesZoom14GateAtHigherZooms()
    {
        var localRoad = CreateFeature("busway");

        Assert.False(HighwayZoomRules.ShouldKeepFeature(localRoad, 13));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(localRoad, 14));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(localRoad, 15));
    }

    [Fact]
    public void GetSimplificationEpsilon_IsRougherAtLowZooms()
    {
        Assert.Equal(0.0180, HighwayZoomRules.GetSimplificationEpsilon(7));
        Assert.Equal(0.0100, HighwayZoomRules.GetSimplificationEpsilon(8));
        Assert.Equal(0.0060, HighwayZoomRules.GetSimplificationEpsilon(9));
        Assert.Equal(0.0030, HighwayZoomRules.GetSimplificationEpsilon(10));
        Assert.Equal(0.0007, HighwayZoomRules.GetSimplificationEpsilon(11));
    }

    private static Feature CreateFeature(string highway, string? trailVisibility = null, string? sacScale = null)
    {
        var properties = new Dictionary<string, dynamic>
        {
            ["highway"] = highway
        };

        if (!string.IsNullOrWhiteSpace(trailVisibility))
            properties["trail_visibility"] = trailVisibility;

        if (!string.IsNullOrWhiteSpace(sacScale))
            properties["sac_scale"] = sacScale;

        return new Feature(
            new LineString(
            [
                new Position(10.0, 59.0),
                new Position(10.01, 59.01)
            ]),
            properties,
            null,
            new FeatureId($"{highway}-{trailVisibility ?? "none"}-{sacScale ?? "none"}"));
    }
}
