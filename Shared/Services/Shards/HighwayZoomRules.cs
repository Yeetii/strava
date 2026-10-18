using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Services.Shards;

public static class HighwayZoomRules
{
    private static readonly HashSet<string> MajorRoads = new(StringComparer.Ordinal)
    {
        "motorway",
        "motorway_link",
        "trunk",
        "trunk_link",
        "primary",
        "primary_link",
        "secondary",
        "secondary_link",
    };

    private static readonly HashSet<string> ArterialRoads = new(StringComparer.Ordinal)
    {
        "tertiary",
        "tertiary_link",
        "unclassified",
        "road",
    };

    private static readonly HashSet<string> LocalRoads = new(StringComparer.Ordinal)
    {
        "residential",
        "living_street",
        "service",
        "services",
        "busway",
        "bus_guideway",
    };

    // Keep paths/tracks visible earlier than in many default map styles.
    private static readonly HashSet<string> Cycleways = new(StringComparer.Ordinal)
    {
        "cycleway",
        "bridleway",
    };

    private static readonly HashSet<string> TrailBackbone = new(StringComparer.Ordinal)
    {
        "path",
        "track",
    };

    private static readonly HashSet<string> LocalPathways = new(StringComparer.Ordinal)
    {
        "footway",
        "pedestrian",
        "steps",
        "corridor",
        "sidewalk",
        "crossing",
        "link",
        "rest_area",
        "escape",
    };

    private static readonly HashSet<string> LowVisibilityTrailValues = new(StringComparer.Ordinal)
    {
        "bad",
        "very_bad",
        "horrible",
        "very_horrible",
        "impassable",
    };

    private static readonly HashSet<string> HighVisibilityTrailValues = new(StringComparer.Ordinal)
    {
        "excellent",
        "good",
        "intermediate",
    };

    private static readonly HashSet<string> LowDifficultySacScales = new(StringComparer.Ordinal)
    {
        "hiking",
        "mountain_hiking",
    };

    private static readonly HashSet<string> MediumDifficultySacScales = new(StringComparer.Ordinal)
    {
        "demanding_mountain_hiking",
        "alpine_hiking",
    };

    private static readonly HashSet<string> HighDifficultySacScales = new(StringComparer.Ordinal)
    {
        "demanding_alpine_hiking",
        "difficult_alpine_hiking",
    };

    private static readonly uint HighwayTagKeyId = ShardEncodingIds.TagIdFromString("highway");
    private static readonly uint FootwayTagKeyId = ShardEncodingIds.TagIdFromString("footway");
    private static readonly uint SnowmobileTagKeyId = ShardEncodingIds.TagIdFromString("snowmobile");
    private static readonly uint InformalTagKeyId = ShardEncodingIds.TagIdFromString("informal");
    private static readonly uint TrailVisibilityTagKeyId = ShardEncodingIds.TagIdFromString("trail_visibility");
    private static readonly uint SacScaleTagKeyId = ShardEncodingIds.TagIdFromString("sac_scale");
    private static readonly Dictionary<uint, string> TagValueLookup = BuildTagValueLookup();

    public static bool ShouldKeepFeature(Feature feature, int zoom)
    {
        var (highway, footway, snowmobile, informal, trailVisibility, sacScale) =
            GetRoadClassifications(feature.Properties);
        return ShouldKeepClassification(highway, footway, snowmobile, informal, trailVisibility, sacScale, zoom, HighwayTileZoomProfile.Default);
    }

    public static bool ShouldKeepFeature(Feature feature, int zoom, HighwayTileZoomProfile profile)
    {
        var (highway, footway, snowmobile, informal, trailVisibility, sacScale) =
            GetRoadClassifications(feature.Properties);
        return ShouldKeepClassification(highway, footway, snowmobile, informal, trailVisibility, sacScale, zoom, profile);
    }

    public static bool ShouldKeepFeature(ShardFeature feature, int zoom)
    {
        string? highway = null;
        string? footway = null;
        string? snowmobile = null;
        string? informal = null;
        string? trailVisibility = null;
        string? sacScale = null;

        foreach (var tag in feature.Tags)
        {
            if (tag.KeyId == HighwayTagKeyId)
                highway = DecodeValue(tag.ValueId);
            else if (tag.KeyId == FootwayTagKeyId)
                footway = DecodeValue(tag.ValueId);
            else if (tag.KeyId == SnowmobileTagKeyId)
                snowmobile = DecodeValue(tag.ValueId);
            else if (tag.KeyId == InformalTagKeyId)
                informal = DecodeValue(tag.ValueId);
            else if (tag.KeyId == TrailVisibilityTagKeyId)
                trailVisibility = DecodeValue(tag.ValueId);
            else if (tag.KeyId == SacScaleTagKeyId)
                sacScale = DecodeValue(tag.ValueId);
        }

        return ShouldKeepClassification(highway, footway, snowmobile, informal, trailVisibility, sacScale, zoom, HighwayTileZoomProfile.Default);
    }

    public static bool ShouldKeepFeature(ShardFeature feature, int zoom, HighwayTileZoomProfile profile)
    {
        string? highway = null;
        string? footway = null;
        string? snowmobile = null;
        string? informal = null;
        string? trailVisibility = null;
        string? sacScale = null;

        foreach (var tag in feature.Tags)
        {
            if (tag.KeyId == HighwayTagKeyId)
                highway = DecodeValue(tag.ValueId);
            else if (tag.KeyId == FootwayTagKeyId)
                footway = DecodeValue(tag.ValueId);
            else if (tag.KeyId == SnowmobileTagKeyId)
                snowmobile = DecodeValue(tag.ValueId);
            else if (tag.KeyId == InformalTagKeyId)
                informal = DecodeValue(tag.ValueId);
            else if (tag.KeyId == TrailVisibilityTagKeyId)
                trailVisibility = DecodeValue(tag.ValueId);
            else if (tag.KeyId == SacScaleTagKeyId)
                sacScale = DecodeValue(tag.ValueId);
        }

        return ShouldKeepClassification(highway, footway, snowmobile, informal, trailVisibility, sacScale, zoom, profile);
    }

    public static HighwayTileZoomStep GetStep(int zoom, HighwayTileZoomProfile profile)
        => profile.Steps
            .OrderBy(step => step.MinZoom)
            .Last(step => step.MinZoom <= zoom);

    public static double GetSimplificationEpsilon(int zoom)
        => GetStep(zoom, HighwayTileZoomProfile.Default).SimplificationEpsilon;

    public static double GetSimplificationEpsilon(int zoom, HighwayTileZoomProfile profile)
        => GetStep(zoom, profile).SimplificationEpsilon;

    public static double GetMinClippedLengthMeters(int zoom, HighwayTileZoomProfile profile)
        => GetStep(zoom, profile).MinClippedLengthMeters;

    private static bool ShouldKeepClassification(
        string? highway,
        string? footway,
        string? snowmobile,
        string? informal,
        string? trailVisibility,
        string? sacScale,
        int zoom,
        HighwayTileZoomProfile profile)
    {
        var values = GetDistinctNonEmptyValues(highway, footway);
        if (HasSnowmobileAccess(snowmobile) && zoom >= profile.SnowmobileMinZoom)
            return true;

        if (values.Count == 0)
            return false;

        return values.Any(value => IsVisibleAtZoom(value, footway, informal, trailVisibility, sacScale, zoom, profile));
    }

    private static bool HasSnowmobileAccess(string? snowmobile)
        => snowmobile is "yes" or "designated" or "permissive" or "official";

    private static bool IsVisibleAtZoom(string value, string? footway, string? informal, string? trailVisibility, string? sacScale, int zoom, HighwayTileZoomProfile profile)
    {
        if (MajorRoads.Contains(value))
            return zoom >= profile.MajorRoadsMinZoom;

        if (ArterialRoads.Contains(value))
            return zoom >= profile.ArterialRoadsMinZoom;

        if (value is "service" or "services")
            return zoom >= profile.ServiceRoadsMinZoom;

        if (value is "residential" or "living_street")
            return zoom >= profile.LocalRoadsMinZoom;

        if (LocalRoads.Contains(value))
            return zoom >= profile.LocalRoadsMinZoom;

        if (IsTrailValue(value))
            return zoom >= GetTrailMinimumZoom(value, footway, informal, trailVisibility, sacScale, profile);

        return false;
    }

    private static int GetTrailMinimumZoom(string value, string? footway, string? informal, string? trailVisibility, string? sacScale, HighwayTileZoomProfile profile)
    {
        var baseZoom = value switch
        {
            _ when IsInformalValue(value, footway, informal) => profile.InformalMinZoom,
            _ when TrailBackbone.Contains(value) => profile.TrailBackboneMinZoom,
            _ when Cycleways.Contains(value) => profile.CyclewayMinZoom,
            _ when LocalPathways.Contains(value) => profile.LocalPathwaysMinZoom,
            _ => int.MaxValue
        };

        if (LowVisibilityTrailValues.Contains(trailVisibility ?? string.Empty))
            baseZoom = Math.Max(baseZoom, profile.LowVisibilityTrailMinZoom);
        if (HighDifficultySacScales.Contains(sacScale ?? string.Empty))
            baseZoom = Math.Max(baseZoom, profile.HighDifficultySacMinZoom);
        else if (MediumDifficultySacScales.Contains(sacScale ?? string.Empty))
            baseZoom = Math.Max(baseZoom, profile.MediumDifficultySacMinZoom);

        return baseZoom;
    }

    private static bool IsTrailValue(string value)
        => TrailBackbone.Contains(value)
            || Cycleways.Contains(value)
            || LocalPathways.Contains(value)
            || string.Equals(value, "informal", StringComparison.Ordinal);

    private static (string? highway, string? footway, string? snowmobile, string? informal, string? trailVisibility, string? sacScale) GetRoadClassifications(IDictionary<string, dynamic> properties)
    {
        var highway = TryReadProperty(properties, "highway");
        var footway = TryReadProperty(properties, "footway");
        var snowmobile = TryReadProperty(properties, "snowmobile");
        var informal = TryReadProperty(properties, "informal");
        var trailVisibility = TryReadProperty(properties, "trail_visibility");
        var sacScale = TryReadProperty(properties, "sac_scale");
        return (highway, footway, snowmobile, informal, trailVisibility, sacScale);
    }

    private static bool IsTruthy(string? value)
        => value is "yes" or "true" or "1";

    private static bool IsInformalValue(string value, string? footway, string? informal)
        => string.Equals(value, "informal", StringComparison.Ordinal)
            || string.Equals(footway, "informal", StringComparison.Ordinal)
            || IsTruthy(informal);

    private static string? TryReadProperty(IDictionary<string, dynamic> properties, string key)
    {
        if (!properties.TryGetValue(key, out var value) || value is null)
            return null;

        var text = value.ToString();
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private static List<string> GetDistinctNonEmptyValues(params string?[] values)
        => [.. values
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value ?? string.Empty)
            .Distinct(StringComparer.Ordinal)];

    private static Dictionary<uint, string> BuildTagValueLookup()
    {
        var values = MajorRoads
            .Concat(ArterialRoads)
            .Concat(LocalRoads)
            .Concat(Cycleways)
            .Concat(LocalPathways)
            .Concat(LowVisibilityTrailValues)
            .Concat(HighVisibilityTrailValues)
            .Concat(LowDifficultySacScales)
            .Concat(MediumDifficultySacScales)
            .Concat(HighDifficultySacScales)
            .Distinct(StringComparer.Ordinal);

        return values
            .ToDictionary(ShardEncodingIds.TagIdFromString, value => value);
    }

    private static string? DecodeValue(uint valueId)
        => TagValueLookup.TryGetValue(valueId, out var value) ? value : null;
}
