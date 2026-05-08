using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services.Shards;

namespace Shared.Tests;

public class ShardStackTests
{
    [Fact]
    public void PackedGeometryCodec_RoundTrips_LineString()
    {
        var line = new LineString(
        [
            new Position(11.1234567, 59.1234567),
            new Position(11.2234567, 59.2234567),
            new Position(11.3234567, 59.3234567)
        ]);

        var (type, bytes) = PackedGeometryCodec.Encode(line);
        var decoded = PackedGeometryCodec.Decode(type, bytes);

        var decodedLine = Assert.IsType<LineString>(decoded);
        var coordinates = decodedLine.Coordinates.ToList();
        Assert.Equal(3, coordinates.Count);
        Assert.Equal(11.1234567, coordinates[0].Longitude, 6);
        Assert.Equal(59.3234567, coordinates[2].Latitude, 6);
    }

    [Fact]
    public void ShardBinarySerializer_RoundTrips_OwnedAndPointers()
    {
        var shard = new Shard
        {
            Owned =
            [
                new ShardFeature
                {
                    Id = 42,
                    Type = ShardFeatureType.LineString,
                    Geometry = [1, 2, 3],
                    Tags = [new ShardTag { KeyId = 1, ValueId = 2 }]
                }
            ],
            Pointers =
            [
                new FeaturePointer { FeatureId = 77, OwnerX = 1, OwnerY = 2, LocalIndex = 3 }
            ]
        };

        var bytes = ShardBinarySerializer.Serialize(shard);
        var roundTrip = ShardBinarySerializer.Deserialize(bytes);

        Assert.Single(roundTrip.Owned);
        Assert.Single(roundTrip.Pointers);
        Assert.Equal((ulong)42, roundTrip.Owned[0].Id);
        Assert.Equal(ShardFeatureType.LineString, roundTrip.Owned[0].Type);
        Assert.Equal((ulong)77, roundTrip.Pointers[0].FeatureId);
        Assert.Equal((uint)3, roundTrip.Pointers[0].LocalIndex);
    }

    [Fact]
    public void BlobTileService_IntersectingShardKeys_Expands_WhenRequestZoomBelowCanonical()
    {
        var keys = BlobTileService.GetIntersectingShardKeys(z: 10, x: 550, y: 330, shardZoom: 12);
        Assert.Equal(16, keys.Count);
        Assert.Contains((2200, 1320), keys);
    }

    [Fact]
    public async Task ShardFeatureClient_ResolvesPointersByOwnerShardBatch()
    {
        var owner = new Shard
        {
            Owned = [new ShardFeature { Id = 10, Type = ShardFeatureType.Point, Geometry = PackedGeometryCodec.Encode(new Point(new Position(10, 10))).bytes }]
        };
        var pointerShard = new Shard
        {
            Pointers = [new FeaturePointer { FeatureId = 10, OwnerX = 1, OwnerY = 2, LocalIndex = 0 }]
        };

        var fakeRepo = new FakeShardRepository(new Dictionary<(int z, int x, int y), Shard>
        {
            [(12, 1, 2)] = owner,
            [(12, 5, 6)] = pointerShard
        });
        var client = new ShardFeatureClient(fakeRepo, canonicalZoom: 12);

        var features = await client.GetFeaturesForShards([(5, 6)], CancellationToken.None);

        Assert.Single(features);
        Assert.Equal(2, fakeRepo.GetCalls); // pointer tile + owner tile
    }

    private sealed class FakeShardRepository(Dictionary<(int z, int x, int y), Shard> shards) : IShardRepository
    {
        public int GetCalls { get; private set; }

        public Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            GetCalls++;
            return Task.FromResult(shards[(z, x, y)]);
        }
    }
}
