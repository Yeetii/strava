using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Services.Shards;

public class ShardFeatureClient(IShardRepository shardRepository, int canonicalZoom = 12)
{
    private readonly IShardRepository _shardRepository = shardRepository;
    private readonly int _canonicalZoom = canonicalZoom;

    public async Task<IReadOnlyList<Feature>> GetFeaturesForShards(
        IEnumerable<(int x, int y)> shardKeys,
        CancellationToken cancellationToken = default)
    {
        var keys = shardKeys.Distinct().ToList();
        if (keys.Count == 0)
            return [];

        var shards = new Dictionary<(int x, int y), Shard>();
        foreach (var (x, y) in keys)
            shards[(x, y)] = await _shardRepository.GetShardAsync(_canonicalZoom, x, y, cancellationToken);

        var features = new List<Feature>();
        var seen = new HashSet<ulong>();
        var pointers = new List<FeaturePointer>();

        foreach (var shard in shards.Values)
        {
            foreach (var owned in shard.Owned)
            {
                if (!seen.Add(owned.Id))
                    continue;
                features.Add(ToFeature(owned));
            }
            pointers.AddRange(shard.Pointers);
        }

        var pointersByOwner = pointers
            .GroupBy(pointer => (x: (int)pointer.OwnerX, y: (int)pointer.OwnerY))
            .ToDictionary(group => group.Key, group => group.ToList());

        foreach (var (ownerKey, ownerPointers) in pointersByOwner)
        {
            var ownerShard = shards.TryGetValue(ownerKey, out var cached)
                ? cached
                : await _shardRepository.GetShardAsync(_canonicalZoom, ownerKey.x, ownerKey.y, cancellationToken);

            var ownedById = ownerShard.Owned.ToDictionary(feature => feature.Id);
            foreach (var pointer in ownerPointers)
            {
                ShardFeature? resolved = null;
                if (pointer.LocalIndex < ownerShard.Owned.Count)
                {
                    var byIndex = ownerShard.Owned[(int)pointer.LocalIndex];
                    if (byIndex.Id == pointer.FeatureId)
                        resolved = byIndex;
                }

                if (resolved is null && ownedById.TryGetValue(pointer.FeatureId, out var byId))
                    resolved = byId;

                if (resolved is null || !seen.Add(resolved.Id))
                    continue;

                features.Add(ToFeature(resolved));
            }
        }

        return features;
    }

    private static Feature ToFeature(ShardFeature feature)
    {
        var geometry = PackedGeometryCodec.Decode(feature.Type, feature.Geometry);
        var properties = new Dictionary<string, dynamic>();
        return new Feature(geometry, properties, null, new FeatureId(feature.Id.ToString()));
    }
}
