using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Services.Shards;

public static class HighwayZoomRules
{
    private const int LowVisibilityPenalty = 2;
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
    private static readonly HashSet<string> TrailNetwork = new(StringComparer.Ordinal)
    {
        "path",
        "track",
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
        "informal",
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
    private static readonly uint TrailVisibilityTagKeyId = ShardEncodingIds.TagIdFromString("trail_visibility");
    private static readonly uint SacScaleTagKeyId = ShardEncodingIds.TagIdFromString("sac_scale");
    private static readonly Dictionary<uint, string> TagValueLookup = BuildTagValueLookup();

    public static bool ShouldKeepFeature(Feature feature, int zoom)
    {
        var (highway, footway, trailVisibility, sacScale) = GetRoadClassifications(feature.Properties);
        return ShouldKeepClassification(highway, footway, trailVisibility, sacScale, zoom);
    }

    public static bool ShouldKeepFeature(ShardFeature feature, int zoom)
    {
        string? highway = null;
        string? footway = null;
        string? trailVisibility = null;
        string? sacScale = null;

        foreach (var tag in feature.Tags)
        {
            if (tag.KeyId == HighwayTagKeyId)
                highway = DecodeValue(tag.ValueId);
            else if (tag.KeyId == FootwayTagKeyId)
                footway = DecodeValue(tag.ValueId);
            else if (tag.KeyId == TrailVisibilityTagKeyId)
                trailVisibility = DecodeValue(tag.ValueId);
            else if (tag.KeyId == SacScaleTagKeyId)
                sacScale = DecodeValue(tag.ValueId);
        }

        return ShouldKeepClassification(highway, footway, trailVisibility, sacScale, zoom);
    }

    public static double GetSimplificationEpsilon(int zoom)
        => zoom switch
        {
            <= 7 => 0.0130,
            8 => 0.0070,
            9 => 0.0040,
            10 => 0.0020,
            11 => 0.0005,
            12 => 0.0002,
            _ => 0d
        };

    private static bool ShouldKeepClassification(string? highway, string? footway, string? trailVisibility, string? sacScale, int zoom)
    {
        var values = GetDistinctNonEmptyValues(highway, footway);
        if (values.Count == 0)
            return false;

        return values.Any(value => IsVisibleAtZoom(value, trailVisibility, sacScale, zoom));
    }

    private static bool IsVisibleAtZoom(string value, string? trailVisibility, string? sacScale, int zoom)
    {
        if (MajorRoads.Contains(value))
            return true;

        if (ArterialRoads.Contains(value))
            return zoom >= 10;

        if (value is "service" or "services")
            return zoom >= 12;

        if (value is "residential" or "living_street")
            return zoom >= 13;

        if (LocalRoads.Contains(value))
            return zoom >= 14;

        if (IsTrailValue(value))
            return zoom >= GetTrailMinimumZoom(value, trailVisibility, sacScale);

        return false;
    }

    private static int GetTrailMinimumZoom(string value, string? trailVisibility, string? sacScale)
    {
        var baseZoom = value switch
        {
            _ when TrailBackbone.Contains(value) => 8,
            _ when TrailNetwork.Contains(value) => 10,
            _ when LocalPathways.Contains(value) => 11,
            _ => int.MaxValue
        };

        return baseZoom + GetTrailVisibilityPenalty(trailVisibility) + GetSacScalePenalty(sacScale);
    }

    private static bool IsTrailValue(string value)
        => TrailBackbone.Contains(value) || TrailNetwork.Contains(value) || LocalPathways.Contains(value);

    private static int GetTrailVisibilityPenalty(string? trailVisibility)
    {
        if (string.IsNullOrWhiteSpace(trailVisibility))
            return 0;

        if (HighVisibilityTrailValues.Contains(trailVisibility))
            return 0;

        return LowVisibilityTrailValues.Contains(trailVisibility)
            ? LowVisibilityPenalty
            : 0;
    }

    private static int GetSacScalePenalty(string? sacScale)
    {
        if (string.IsNullOrWhiteSpace(sacScale))
            return 0;

        if (LowDifficultySacScales.Contains(sacScale))
            return 0;
        if (HighDifficultySacScales.Contains(sacScale))
            return 2;
        if (MediumDifficultySacScales.Contains(sacScale))
            return 1;

        return 0;
    }

    private static (string? highway, string? footway, string? trailVisibility, string? sacScale) GetRoadClassifications(IDictionary<string, dynamic> properties)
    {
        var highway = TryReadProperty(properties, "highway");
        var footway = TryReadProperty(properties, "footway");
        var trailVisibility = TryReadProperty(properties, "trail_visibility");
        var sacScale = TryReadProperty(properties, "sac_scale");
        return (highway, footway, trailVisibility, sacScale);
    }

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
            .Concat(TrailNetwork)
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
