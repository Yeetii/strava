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
    private static readonly HashSet<string> TrailNetwork = new(StringComparer.Ordinal)
    {
        "path",
        "track",
        "cycleway",
        "bridleway",
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

    private static readonly uint HighwayTagKeyId = ShardEncodingIds.TagIdFromString("highway");
    private static readonly uint FootwayTagKeyId = ShardEncodingIds.TagIdFromString("footway");
    private static readonly Dictionary<uint, string> TagValueLookup = BuildTagValueLookup();

    public static bool ShouldKeepFeature(Feature feature, int zoom)
    {
        var (highway, footway) = GetRoadClassifications(feature.Properties);
        return ShouldKeepClassification(highway, footway, zoom);
    }

    public static bool ShouldKeepFeature(ShardFeature feature, int zoom)
    {
        string? highway = null;
        string? footway = null;

        foreach (var tag in feature.Tags)
        {
            if (tag.KeyId == HighwayTagKeyId)
                highway = DecodeValue(tag.ValueId);
            else if (tag.KeyId == FootwayTagKeyId)
                footway = DecodeValue(tag.ValueId);
        }

        return ShouldKeepClassification(highway, footway, zoom);
    }

    public static double GetSimplificationEpsilon(int zoom)
        => zoom switch
        {
            <= 7 => 0.0100,
            8 => 0.0060,
            9 => 0.0030,
            10 => 0.0015,
            11 => 0.0007,
            _ => 0d
        };

    private static bool ShouldKeepClassification(string? highway, string? footway, int zoom)
    {
        if (zoom >= 12)
            return true;

        var values = GetDistinctNonEmptyValues(highway, footway);
        if (values.Count == 0)
            return false;

        if (zoom <= 7)
            return values.Any(IsLowZoomCore);
        if (zoom == 8)
            return values.Any(IsZoom8Visible);
        if (zoom == 9)
            return values.Any(IsZoom9Visible);
        if (zoom == 10)
            return values.Any(IsZoom10Visible);

        return values.Any(IsZoom11Visible);
    }

    private static bool IsLowZoomCore(string value)
        => MajorRoads.Contains(value) || TrailNetwork.Contains(value);

    private static bool IsZoom8Visible(string value)
        => IsLowZoomCore(value) || ArterialRoads.Contains(value);

    private static bool IsZoom9Visible(string value)
        => IsZoom8Visible(value) || value is "residential" or "living_street" or "road";

    private static bool IsZoom10Visible(string value)
        => IsZoom9Visible(value) || value is "service" or "services" or "footway" or "pedestrian" or "steps";

    private static bool IsZoom11Visible(string value)
        => IsZoom10Visible(value) || LocalRoads.Contains(value) || LocalPathways.Contains(value);

    private static (string? highway, string? footway) GetRoadClassifications(IDictionary<string, dynamic> properties)
    {
        var highway = TryReadProperty(properties, "highway");
        var footway = TryReadProperty(properties, "footway");
        return (highway, footway);
    }

    private static string? TryReadProperty(IDictionary<string, dynamic> properties, string key)
    {
        if (!properties.TryGetValue(key, out var value) || value is null)
            return null;

        var text = value.ToString();
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private static List<string> GetDistinctNonEmptyValues(params string?[] values)
        => values
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value ?? string.Empty)
            .Distinct(StringComparer.Ordinal)
            .ToList();

    private static Dictionary<uint, string> BuildTagValueLookup()
    {
        var values = MajorRoads
            .Concat(ArterialRoads)
            .Concat(LocalRoads)
            .Concat(TrailNetwork)
            .Concat(LocalPathways)
            .Distinct(StringComparer.Ordinal);

        return values
            .ToDictionary(ShardEncodingIds.TagIdFromString, value => value);
    }

    private static string? DecodeValue(uint valueId)
        => TagValueLookup.TryGetValue(valueId, out var value) ? value : null;
}
