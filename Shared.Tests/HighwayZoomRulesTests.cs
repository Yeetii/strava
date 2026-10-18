using BAMCIS.GeoJSON;
using Shared.Services.Shards;

namespace Shared.Tests;

public class HighwayZoomRulesTests
{
    [Fact]
    public void ShouldKeepFeature_KeepsOnlyMajorRoadsAtZoom7()
    {
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("primary"), 7));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("path"), 7));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("tertiary"), 7));
    }

    [Fact]
    public void ShouldKeepFeature_ShowsHighVisibilityTrailBackboneBeforeLowVisibility()
    {
        var highVisibilityPath = CreateFeature("path", trailVisibility: "good");
        var lowVisibilityPath = CreateFeature("path", trailVisibility: "bad");

        Assert.True(HighwayZoomRules.ShouldKeepFeature(highVisibilityPath, 7));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(lowVisibilityPath, 7));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(lowVisibilityPath, 9));
    }

    [Fact]
    public void ShouldKeepFeature_UsesSacScaleToDelayDemandingTrails()
    {
        var easyTrack = CreateFeature("track", sacScale: "hiking");
        var demandingTrack = CreateFeature("track", sacScale: "demanding_alpine_hiking");

        Assert.True(HighwayZoomRules.ShouldKeepFeature(easyTrack, 7));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(demandingTrack, 8));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(demandingTrack, 9));
    }

    [Fact]
    public void ShouldKeepFeature_DefersMinorRoadsUntilLaterZooms()
    {
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("tertiary"), 9));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("tertiary"), 10));
        Assert.False(HighwayZoomRules.ShouldKeepFeature(CreateFeature("service"), 11));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("service"), 12));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(CreateFeature("residential"), 12));
    }

    [Fact]
    public void ShouldKeepFeature_HidesInformalUntilZoom13()
    {
        var informal = CreateFeature("path", informal: "yes");

        Assert.False(HighwayZoomRules.ShouldKeepFeature(informal, 12));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(informal, 13));
    }

    [Fact]
    public void ShouldKeepFeature_HidesFootwayInformalUntilZoom13()
    {
        var informalFootway = CreateFeature("footway", footway: "informal");

        Assert.False(HighwayZoomRules.ShouldKeepFeature(informalFootway, 12));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(informalFootway, 13));
    }

    [Fact]
    public void ShouldKeepFeature_AppliesZoom14GateAtHigherZooms()
    {
        var localRoad = CreateFeature("busway");

        Assert.False(HighwayZoomRules.ShouldKeepFeature(localRoad, 11));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(localRoad, 12));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(localRoad, 15));
    }

    [Fact]
    public void ShouldKeepFeature_KeepsSnowmobileOnlyWaysAtTrailBackboneZooms()
    {
        var snowmobileOnly = CreateFeature(highway: null, snowmobile: "yes");

        Assert.False(HighwayZoomRules.ShouldKeepFeature(snowmobileOnly, 6));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(snowmobileOnly, 7));
    }

    [Fact]
    public void ShouldKeepFeature_KeepsSnowmobileTaggedRoadsAtTrailBackboneZooms()
    {
        var snowmobileResidential = CreateFeature(highway: "residential", snowmobile: "yes");

        Assert.False(HighwayZoomRules.ShouldKeepFeature(snowmobileResidential, 6));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(snowmobileResidential, 7));
    }

    [Theory]
    [InlineData("designated")]
    [InlineData("permissive")]
    [InlineData("official")]
    public void ShouldKeepFeature_TreatsAdditionalSnowmobileAccessValuesAsVisible(string snowmobile)
    {
        var snowmobileOnly = CreateFeature(highway: null, snowmobile: snowmobile);

        Assert.False(HighwayZoomRules.ShouldKeepFeature(snowmobileOnly, 6));
        Assert.True(HighwayZoomRules.ShouldKeepFeature(snowmobileOnly, 7));
    }

    [Fact]
    public void GetSimplificationEpsilon_IsRougherAtLowZooms()
    {
        Assert.Equal(0.0130, HighwayZoomRules.GetSimplificationEpsilon(7));
        Assert.Equal(0.0070, HighwayZoomRules.GetSimplificationEpsilon(8));
        Assert.Equal(0.0040, HighwayZoomRules.GetSimplificationEpsilon(9));
        Assert.Equal(0.0020, HighwayZoomRules.GetSimplificationEpsilon(10));
        Assert.Equal(0.0005, HighwayZoomRules.GetSimplificationEpsilon(11));
        Assert.Equal(0.00015, HighwayZoomRules.GetSimplificationEpsilon(12));
        Assert.Equal(0.00008, HighwayZoomRules.GetSimplificationEpsilon(13));
        Assert.Equal(0.00001, HighwayZoomRules.GetSimplificationEpsilon(15));
        Assert.Equal(0.000003, HighwayZoomRules.GetSimplificationEpsilon(16));
    }

    private static Feature CreateFeature(
        string? highway,
        string? trailVisibility = null,
        string? sacScale = null,
        string? snowmobile = null,
        string? informal = null,
        string? footway = null)
    {
        var properties = new Dictionary<string, dynamic>();

        if (!string.IsNullOrWhiteSpace(highway))
            properties["highway"] = highway;

        if (!string.IsNullOrWhiteSpace(trailVisibility))
            properties["trail_visibility"] = trailVisibility;

        if (!string.IsNullOrWhiteSpace(sacScale))
            properties["sac_scale"] = sacScale;

        if (!string.IsNullOrWhiteSpace(snowmobile))
            properties["snowmobile"] = snowmobile;

        if (!string.IsNullOrWhiteSpace(informal))
            properties["informal"] = informal;

        if (!string.IsNullOrWhiteSpace(footway))
            properties["footway"] = footway;

        return new Feature(
            new LineString(
            [
                new Position(10.0, 59.0),
                new Position(10.01, 59.01)
            ]),
            properties,
            null,
            new FeatureId($"{highway ?? "snowmobile"}-{trailVisibility ?? "none"}-{sacScale ?? "none"}-{snowmobile ?? "none"}"));
    }
}
