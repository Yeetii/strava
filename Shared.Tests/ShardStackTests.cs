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
    public void ShardBinarySerializer_RoundTrips_OwnedFeatures_WithOsmId()
    {
        var shard = new Shard
        {
            Owned =
            [
                new ShardFeature
                {
                    Id = 42,
                    OsmId = "123456",
                    Name = "My Trail",
                    Type = ShardFeatureType.LineString,
                    Geometry = [1, 2, 3],
                    Tags = [new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("surface"), ValueId = ShardEncodingIds.TagIdFromString("gravel") }]
                }
            ]
        };

        var bytes = ShardBinarySerializer.Serialize(shard);
        var roundTrip = ShardBinarySerializer.Deserialize(bytes);

        Assert.Single(roundTrip.Owned);
        Assert.Equal((ulong)42, roundTrip.Owned[0].Id);
        Assert.Equal("123456", roundTrip.Owned[0].OsmId);
        Assert.Equal("My Trail", roundTrip.Owned[0].Name);
        Assert.Equal(ShardFeatureType.LineString, roundTrip.Owned[0].Type);
        Assert.Collection(roundTrip.Owned[0].Tags,
            tag =>
            {
                Assert.Equal(ShardEncodingIds.TagIdFromString("surface"), tag.KeyId);
                Assert.Equal(ShardEncodingIds.TagIdFromString("gravel"), tag.ValueId);
            });
    }

    [Fact]
    public void BlobTileService_IntersectingShardKeys_Expands_WhenRequestZoomBelowCanonical()
    {
        var keys = BlobTileService.GetIntersectingShardKeys(z: 10, x: 550, y: 330, shardZoom: 12);
        Assert.Equal(16, keys.Count);
        Assert.Contains((2200, 1320), keys);
    }

    [Fact]
    public async Task ShardFeatureClient_ExposesOsmIdProperty()
    {
        var shard = new Shard
        {
            Owned = [new ShardFeature
            {
                Id = 10,
                OsmId = "987654",
                Name = "Residential Connector",
                Type = ShardFeatureType.Point,
                Geometry = PackedGeometryCodec.Encode(new Point(new Position(10, 10))).bytes,
                Tags =
                [
                    new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("highway"), ValueId = ShardEncodingIds.TagIdFromString("residential") },
                    new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("oneway"), ValueId = ShardEncodingIds.TagIdFromString("yes") },
                    new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("footway"), ValueId = ShardEncodingIds.TagIdFromString("informal") },
                    new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("surface"), ValueId = ShardEncodingIds.TagIdFromString("dirt") },
                    new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("width"), ValueId = ShardEncodingIds.TagIdFromString("1.5") },
                    new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("trail_visibility"), ValueId = ShardEncodingIds.TagIdFromString("intermediate") },
                    new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("sac_scale"), ValueId = ShardEncodingIds.TagIdFromString("mountain_hiking") }
                ]
            }]
        };

        var fakeRepo = new FakeShardRepository(new Dictionary<(int z, int x, int y), Shard>
        {
            [(12, 5, 6)] = shard
        });
        var client = new ShardFeatureClient(fakeRepo, Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardFeatureClient>.Instance, canonicalZoom: 12);

        var features = await client.GetFeaturesForShards([(5, 6)], CancellationToken.None);

        Assert.Single(features);
        Assert.Equal("987654", Assert.IsType<string>(features[0].Properties["osmId"]));
        Assert.Equal("Residential Connector", Assert.IsType<string>(features[0].Properties["name"]));
        Assert.Equal("residential", Assert.IsType<string>(features[0].Properties["highway"]));
        Assert.Equal("yes", Assert.IsType<string>(features[0].Properties["oneway"]));
        Assert.Equal("informal", Assert.IsType<string>(features[0].Properties["footway"]));
        Assert.Equal("dirt", Assert.IsType<string>(features[0].Properties["surface"]));
        Assert.Equal("1.5", Assert.IsType<string>(features[0].Properties["width"]));
        Assert.Equal("intermediate", Assert.IsType<string>(features[0].Properties["trail_visibility"]));
        Assert.Equal("mountain_hiking", Assert.IsType<string>(features[0].Properties["sac_scale"]));
        Assert.Equal(1, fakeRepo.GetCalls);
    }

    [Fact]
    public void MvtTileEncoder_Encodes_StringProperties()
    {
        var feature = new Feature(
            new LineString(
            [
                new Position(0, 0),
                new Position(1, 1)
            ]),
            new Dictionary<string, dynamic> { ["osmId"] = "way-123" },
            null,
            new FeatureId("1"));

        var tile = MvtTileEncoder.EncodeLayer("highways", [feature], 0, 0, 0);
        var utf8 = System.Text.Encoding.UTF8.GetString(tile);

        Assert.Contains("osmId", utf8);
        Assert.Contains("way-123", utf8);
    }

    [Fact]
    public void BlobTileService_FilterByZoom_HidesMinorUrbanRoadsAtLowZoom_ButKeepsTracks()
    {
        var residential = new Feature(
            new LineString([new Position(10, 59), new Position(10.01, 59.01)]),
            new Dictionary<string, dynamic> { ["highway"] = "residential" },
            null,
            new FeatureId("r1"));

        var track = new Feature(
            new LineString([new Position(10, 59), new Position(10.01, 59.01)]),
            new Dictionary<string, dynamic> { ["highway"] = "track" },
            null,
            new FeatureId("t1"));

        var visible = BlobTileService.FilterByZoom([residential, track], zoom: 8).ToList();

        Assert.Single(visible);
        Assert.Equal("track", Assert.IsType<string>(visible[0].Properties["highway"]));
    }

    [Fact]
    public void BlobTileService_SimplifyByZoom_ReducesVerticesAtLowerZoom()
    {
        var points = new List<Position>();
        for (var i = 0; i < 80; i++)
        {
            var lon = 10 + (i * 0.0002);
            var lat = 59 + Math.Sin(i * 0.35) * 0.0015;
            points.Add(new Position(lon, lat));
        }

        var feature = new Feature(
            new LineString(points),
            new Dictionary<string, dynamic> { ["highway"] = "track" },
            null,
            new FeatureId("line1"));

        var simplified = BlobTileService.SimplifyByZoom([feature], zoom: 8).Single();
        var line = Assert.IsType<LineString>(simplified.Geometry);

        Assert.True(line.Coordinates.Count() < points.Count);
    }

    [Fact]
    public void BlobTileService_FilteredTilePayload_IsSmallerAtLowerZooms()
    {
        var features = new[]
        {
            CreateHighwayFeature("primary", "p1"),
            CreateHighwayFeature("tertiary", "t1"),
            CreateHighwayFeature("path", "path-good", trailVisibility: "good"),
            CreateHighwayFeature("path", "path-bad", trailVisibility: "bad"),
            CreateHighwayFeature("footway", "foot-bad", trailVisibility: "bad"),
            CreateHighwayFeature("residential", "r1")
        };

        var zoom8 = BlobTileService.FilterByZoom(features, 8).ToList();
        var zoom12 = BlobTileService.FilterByZoom(features, 12).ToList();

        var zoom8Tile = MvtTileEncoder.EncodeLayer("highways", BlobTileService.SimplifyByZoom(zoom8, 8), 0, 0, 0);
        var zoom12Tile = MvtTileEncoder.EncodeLayer("highways", BlobTileService.SimplifyByZoom(zoom12, 12), 0, 0, 0);

        Assert.True(zoom8.Count < zoom12.Count);
        Assert.True(zoom8Tile.Length < zoom12Tile.Length);
    }

    [Fact]
    public void HighwayZoomIndexService_IndexBlobPath_IsStable()
    {
        var path = HighwayZoomIndexService.GetIndexBlobPath(9, 277, 168);
        Assert.Equal("index/9/277/168.json", path);
    }

    private static Feature CreateHighwayFeature(string highway, string id, string? trailVisibility = null)
    {
        var properties = new Dictionary<string, dynamic>
        {
            ["highway"] = highway
        };

        if (!string.IsNullOrWhiteSpace(trailVisibility))
            properties["trail_visibility"] = trailVisibility;

        return new Feature(
            new LineString([new Position(10, 59), new Position(10.01, 59.01)]),
            properties,
            null,
            new FeatureId(id));
    }

    private sealed class FakeShardRepository(Dictionary<(int z, int x, int y), Shard> shards) : IShardRepository
    {
        public int GetCalls { get; private set; }
        public int DeleteCalls { get; private set; }

        public Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            GetCalls++;
            return Task.FromResult(shards[(z, x, y)]);
        }

        public Task<Shard?> TryGetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            GetCalls++;
            shards.TryGetValue((z, x, y), out var shard);
            return Task.FromResult(shard);
        }

        public Task DeleteShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            DeleteCalls++;
            shards.Remove((z, x, y));
            return Task.CompletedTask;
        }
    }
}
