using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Services.Shards;

public class ShardFeatureClient(IShardRepository shardRepository, int canonicalZoom = 12)
{
    private readonly IShardRepository _shardRepository = shardRepository;
    private readonly int _canonicalZoom = canonicalZoom;
    public int CanonicalZoom => _canonicalZoom;
    private static readonly string[] RenderValues =
    [
        "yes",
        "no",
        "motorway",
        "motorway_link",
        "trunk",
        "trunk_link",
        "primary",
        "primary_link",
        "secondary",
        "secondary_link",
        "tertiary",
        "tertiary_link",
        "residential",
        "living_street",
        "unclassified",
        "road",
        "escape",
        "busway",
        "bus_guideway",
        "path",
        "footway",
        "track",
        "service",
        "services",
        "rest_area",
        "raceway",
        "cycleway",
        "bridleway",
        "steps",
        "pedestrian",
        "corridor",
        "informal",
        "construction",
        "proposed",
        "sidewalk",
        "crossing",
        "link",
        "unpaved",
        "paved",
        "asphalt",
        "concrete",
        "paving_stones",
        "sett",
        "cobblestone",
        "compacted",
        "fine_gravel",
        "gravel",
        "pebblestone",
        "rock",
        "stones",
        "ground",
        "dirt",
        "earth",
        "mud",
        "sand",
        "grass",
        "grass_paver",
        "woodchips",
        "boardwalk",
        "wood",
        "excellent",
        "good",
        "intermediate",
        "bad",
        "very_bad",
        "horrible",
        "very_horrible",
        "impassable",
        "hiking",
        "mountain_hiking",
        "demanding_mountain_hiking",
        "alpine_hiking",
        "demanding_alpine_hiking",
        "difficult_alpine_hiking"
    ];

    private static readonly string[] WidthCandidates =
    [
        "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9",
        "1", "1.0", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "1.8", "1.9",
        "2", "2.0", "2.1", "2.2", "2.3", "2.4", "2.5", "2.6", "2.7", "2.8", "2.9",
        "3", "3.0", "3.5", "4", "4.0", "4.5", "5", "5.0", "6", "6.0"
    ];

    private static readonly Dictionary<uint, string> RenderPropertyKeys = CreateRenderPropertyKeys();
    private static readonly Dictionary<uint, string> RenderPropertyValues = CreateRenderPropertyValues();

    public async Task<IReadOnlyList<Feature>> GetFeaturesForShards(
        IEnumerable<(int x, int y)> shardKeys,
        CancellationToken cancellationToken = default)
    {
        var keys = shardKeys.Distinct().ToList();
        if (keys.Count == 0)
            return [];

        // Fetch all shards in parallel to avoid timeout on low zoom levels (which require many shards)
        var shardTasks = keys.Select(key => GetShardWithContext(key, cancellationToken)).ToList();
        var shards = await Task.WhenAll(shardTasks);

        var features = new List<Feature>();
        var seen = new HashSet<ulong>();

        foreach (var shard in shards)
        {
            foreach (var owned in shard.Owned)
            {
                if (!seen.Add(owned.Id))
                    continue;
                features.Add(ToFeature(owned));
            }
        }

        return features;
    }

    public async Task RefreshShards(IEnumerable<(int x, int y)> shardKeys, CancellationToken cancellationToken = default)
    {
        var keys = shardKeys.Distinct().ToList();
        var deleteTasks = keys.Select(key => DeleteShardWithContext(key, cancellationToken)).ToList();
        await Task.WhenAll(deleteTasks);
    }

    private async Task<Shard> GetShardWithContext((int x, int y) key, CancellationToken cancellationToken)
    {
        try
        {
            return await _shardRepository.GetShardAsync(_canonicalZoom, key.x, key.y, cancellationToken);
        }
        catch (OperationCanceledException ex)
        {
            throw new OperationCanceledException($"Cancelled while loading shard z{_canonicalZoom}/{key.x}/{key.y}.", ex, cancellationToken);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to load shard z{_canonicalZoom}/{key.x}/{key.y}.", ex);
        }
    }

    private async Task DeleteShardWithContext((int x, int y) key, CancellationToken cancellationToken)
    {
        try
        {
            await _shardRepository.DeleteShardAsync(_canonicalZoom, key.x, key.y, cancellationToken);
        }
        catch (OperationCanceledException ex)
        {
            throw new OperationCanceledException($"Cancelled while deleting shard z{_canonicalZoom}/{key.x}/{key.y}.", ex, cancellationToken);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to delete shard z{_canonicalZoom}/{key.x}/{key.y}.", ex);
        }
    }

    private static Feature ToFeature(ShardFeature feature)
    {
        var geometry = PackedGeometryCodec.Decode(feature.Type, feature.Geometry);
        var properties = new Dictionary<string, dynamic>();

        if (!string.IsNullOrWhiteSpace(feature.OsmId))
            properties["osmId"] = feature.OsmId;
        if (!string.IsNullOrWhiteSpace(feature.Name))
            properties["name"] = feature.Name;

        foreach (var tag in feature.Tags)
        {
            if (!RenderPropertyKeys.TryGetValue(tag.KeyId, out var key))
                continue;

            if (TryDecodeTagValue(key, tag.ValueId, out var value))
                properties[key] = value;
        }

        return new Feature(geometry, properties, null, new FeatureId(feature.Id.ToString()));
    }

    private static bool TryDecodeTagValue(string key, uint valueId, out string value)
    {
        if (RenderPropertyValues.TryGetValue(valueId, out value!))
            return true;

        if (key == "width")
            return TryDecodeWidth(valueId, out value);

        value = string.Empty;
        return false;
    }

    private static bool TryDecodeWidth(uint valueId, out string value)
    {
        foreach (var candidate in WidthCandidates)
        {
            if (ShardEncodingIds.TagIdFromString(candidate) == valueId)
            {
                value = candidate;
                return true;
            }
        }

        value = string.Empty;
        return false;
    }

    private static Dictionary<uint, string> CreateRenderPropertyKeys()
        => new()
        {
            [ShardEncodingIds.TagIdFromString("highway")] = "highway",
            [ShardEncodingIds.TagIdFromString("footway")] = "footway",
            [ShardEncodingIds.TagIdFromString("surface")] = "surface",
            [ShardEncodingIds.TagIdFromString("width")] = "width",
            [ShardEncodingIds.TagIdFromString("smoothness")] = "smoothness",
            [ShardEncodingIds.TagIdFromString("trail_visibility")] = "trail_visibility",
            [ShardEncodingIds.TagIdFromString("sac_scale")] = "sac_scale"
        };

    private static Dictionary<uint, string> CreateRenderPropertyValues()
        => RenderValues
            .Distinct(StringComparer.Ordinal)
            .ToDictionary(ShardEncodingIds.TagIdFromString, value => value);
}
