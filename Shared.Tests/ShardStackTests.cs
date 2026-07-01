using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BAMCIS.GeoJSON;
using Moq;
using Shared.Geo;
using Shared.Models;
using Shared.Services.Shards;
using System.Text.Json;

namespace Shared.Tests;

public class ShardStackTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

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
    public async Task ShardFeatureClient_GetFeaturesForShards_DoesNotThrowDuringConcurrentEviction()
    {
        var shards = Enumerable.Range(0, 220)
            .ToDictionary(
                i => (12, i, i),
                i => CreateShard(CreateHighwayFeatureForTile(12, i, i, "track", $"way-{i}")));
        var fakeRepo = new SlowFakeShardRepository(shards);
        var client = new ShardFeatureClient(
            fakeRepo,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardFeatureClient>.Instance,
            canonicalZoom: 12);

        var tasks = Enumerable.Range(0, 8)
            .Select(_ => client.GetFeaturesForShards(shards.Keys.Select(key => (key.Item2, key.Item3)), CancellationToken.None));

        var results = await Task.WhenAll(tasks);

        Assert.All(results, features => Assert.NotEmpty(features));
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

    [Fact]
    public async Task BlobTileService_RefreshTileAsync_BelowCanonical_DoesNotRefreshShards()
    {
        var featureClient = new RecordingShardFeatureClient();
        var zoomIndexService = new RecordingHighwayZoomIndexService(selection: new HighwayTileShardSelection(false, [(2200, 1320)]));
        var service = new BlobTileService(
            featureClient,
            zoomIndexService,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<BlobTileService>.Instance,
            shardZoom: 12);

        await service.RefreshTileAsync(10, 550, 330, CancellationToken.None);

        Assert.Equal(0, featureClient.RefreshCalls);
        Assert.Single(zoomIndexService.RebuildRequests);
        Assert.Equal((10, 550, 330), zoomIndexService.RebuildRequests[0]);
    }

    [Fact]
    public async Task HighwayZoomIndexService_GetShardKeysAsync_ReusesCachedIncompleteIndex()
    {
        var blobName = HighwayZoomIndexService.GetIndexBlobPath(7, 68, 38);
        var container = new Mock<BlobContainerClient>();
        var blob = new Mock<BlobClient>();
        var index = new HighwayTileShardIndex(
            Version: 4,
            Zoom: 7,
            X: 68,
            Y: 38,
            IsComplete: false,
            Shards: [new HighwayTileShardRef(2200, 1320), new HighwayTileShardRef(2201, 1320)]);
        var payload = JsonSerializer.SerializeToUtf8Bytes(index, JsonOptions);

        container
            .Setup(client => client.GetBlobClient(blobName))
            .Returns(blob.Object);

        blob
            .Setup(client => client.DownloadContentAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(Response.FromValue(
                BlobsModelFactory.BlobDownloadResult(BinaryData.FromBytes(payload)),
                Mock.Of<Response>()));

        var shardRepository = new FakeShardRepository(new Dictionary<(int z, int x, int y), Shard>());
        var service = new HighwayZoomIndexService(
            container.Object,
            shardRepository,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<HighwayZoomIndexService>.Instance,
            canonicalZoom: 12);

        var selection = await service.GetShardKeysAsync(7, 68, 38, CancellationToken.None);

        Assert.False(selection.IsComplete);
        Assert.Equal([(2200, 1320), (2201, 1320)], selection.Shards);
        Assert.Equal(0, shardRepository.GetCalls);
        blob.Verify(client => client.UploadAsync(It.IsAny<BinaryData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task BlobShardRepository_GetShardAsync_UpdatesExistingIndexesWhenShardMaterializes()
    {
        var container = new InMemoryBlobContainer();
        var shardPath = BlobShardRepository.GetBlobPath(12, 2200, 1320);
        container.SeedBlob(shardPath, BinaryData.FromBytes(ShardBinarySerializer.Serialize(new Shard())));
        var indexPath = HighwayZoomIndexService.GetIndexBlobPath(7, 68, 41);
        container.SeedBlob(indexPath, BinaryData.FromBytes(JsonSerializer.SerializeToUtf8Bytes(
            new HighwayTileShardIndex(4, 7, 68, 41, false, []),
            JsonOptions)));

        var repository = CreateBlobShardRepositoryWithIndexSync(container);

        var loaded = await repository.GetShardAsync(12, 2200, 1320, CancellationToken.None);

        Assert.Single(loaded.Owned);
        var index = JsonSerializer.Deserialize<HighwayTileShardIndex>(container.GetContent(indexPath), JsonOptions);
        Assert.NotNull(index);
        Assert.Contains(index.Shards, shardRef => shardRef.X == 2200 && shardRef.Y == 1320);
    }

    [Fact]
    public async Task BlobShardRepository_DeleteShardAsync_RemovesShardFromExistingIndexes()
    {
        var container = new InMemoryBlobContainer();
        var shardPath = BlobShardRepository.GetBlobPath(12, 2200, 1320);
        var shard = CreateShard(CreateHighwayFeature("track", "way-1"));
        container.SeedBlob(shardPath, BinaryData.FromBytes(ShardBinarySerializer.Serialize(shard)));

        var indexPath = HighwayZoomIndexService.GetIndexBlobPath(7, 68, 41);
        container.SeedBlob(indexPath, BinaryData.FromBytes(JsonSerializer.SerializeToUtf8Bytes(
            new HighwayTileShardIndex(4, 7, 68, 41, false, [new HighwayTileShardRef(2200, 1320)]),
            JsonOptions)));

        var repository = CreateBlobShardRepositoryWithIndexSync(container);

        await repository.DeleteShardAsync(12, 2200, 1320, CancellationToken.None);

        Assert.False(container.Contains(shardPath));
        var index = JsonSerializer.Deserialize<HighwayTileShardIndex>(container.GetContent(indexPath), JsonOptions);
        Assert.NotNull(index);
        Assert.DoesNotContain(index.Shards, shardRef => shardRef.X == 2200 && shardRef.Y == 1320);
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

    private static Shard CreateShard(params Feature[] features)
    {
        var shard = new Shard();
        foreach (var feature in features)
        {
            var (featureType, geometryBytes) = PackedGeometryCodec.Encode(feature.Geometry);
            shard.Owned.Add(new ShardFeature
            {
                Id = ShardEncodingIds.FeatureIdFromString(feature.Id.Value?.ToString() ?? Guid.NewGuid().ToString("N")),
                OsmId = feature.Id.Value?.ToString(),
                Type = featureType,
                Geometry = geometryBytes,
                Tags = [.. feature.Properties.Select(property => new ShardTag
                {
                    KeyId = ShardEncodingIds.TagIdFromString(property.Key),
                    ValueId = ShardEncodingIds.TagIdFromString(property.Value?.ToString() ?? string.Empty)
                })]
            });
        }

        return shard;
    }

    private static BlobShardRepository CreateBlobShardRepositoryWithIndexSync(InMemoryBlobContainer container)
    {
        HighwayZoomIndexService? indexService = null;
        var repository = new BlobShardRepository(
            container.Client.Object,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<BlobShardRepository>.Instance,
            (_, _, _) => Task.FromResult<IEnumerable<Feature>>([CreateHighwayFeatureForTile(12, 2200, 1320, "track", "way-1")]),
            canonicalZoom: 12,
            overlapBufferMeters: 200,
            onShardChanged: async (x, y, shard, cancellationToken) =>
            {
                await indexService!.SyncIndexesForShardAsync(x, y, shard, cancellationToken);
            });

        indexService = new HighwayZoomIndexService(
            container.Client.Object,
            repository,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<HighwayZoomIndexService>.Instance,
            canonicalZoom: 12);

        return repository;
    }

    private class FakeShardRepository(Dictionary<(int z, int x, int y), Shard> shards) : IShardRepository
    {
        public int GetCalls { get; private set; }
        public int DeleteCalls { get; private set; }

        public virtual Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            GetCalls++;
            return Task.FromResult(shards[(z, x, y)]);
        }

        public virtual Task<Shard?> TryGetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            GetCalls++;
            shards.TryGetValue((z, x, y), out var shard);
            return Task.FromResult(shard);
        }

        public virtual Task<DateTimeOffset?> TryGetShardLastModifiedAsync(
            int z,
            int x,
            int y,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult<DateTimeOffset?>(DateTimeOffset.UnixEpoch);
        }

        public virtual Task<Shard> RebuildShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(shards[(z, x, y)]);
        }

        public virtual Task DeleteShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            DeleteCalls++;
            shards.Remove((z, x, y));
            return Task.CompletedTask;
        }
    }

    private sealed class SlowFakeShardRepository(Dictionary<(int z, int x, int y), Shard> shards) : FakeShardRepository(shards)
    {
        public override async Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            await Task.Delay(1, cancellationToken);
            return await base.GetShardAsync(z, x, y, cancellationToken);
        }
    }

    private sealed class RecordingShardFeatureClient() : ShardFeatureClient(
        new FakeShardRepository(new Dictionary<(int z, int x, int y), Shard>()),
        Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardFeatureClient>.Instance,
        canonicalZoom: 12)
    {
        public int RefreshCalls { get; private set; }

        public override Task<IReadOnlyList<Feature>> GetFeaturesForShards(
            IEnumerable<(int x, int y)> shardKeys,
            CancellationToken cancellationToken = default)
        {
            IReadOnlyList<Feature> features = [];
            return Task.FromResult(features);
        }

        public override Task RefreshShards(IEnumerable<(int x, int y)> shardKeys, CancellationToken cancellationToken = default)
        {
            RefreshCalls++;
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingHighwayZoomIndexService(HighwayTileShardSelection selection)
        : HighwayZoomIndexService(
            new Azure.Storage.Blobs.BlobContainerClient("UseDevelopmentStorage=true", "ignored"),
            new FakeShardRepository(new Dictionary<(int z, int x, int y), Shard>()),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<HighwayZoomIndexService>.Instance,
            canonicalZoom: 12)
    {
        private readonly HighwayTileShardSelection _selection = selection;
        public List<(int z, int x, int y)> RebuildRequests { get; } = [];

        public override Task<HighwayTileShardSelection> GetShardKeysAsync(int z, int x, int y, CancellationToken cancellationToken = default)
            => Task.FromResult(_selection);

        public override Task<HighwayTileShardSelection> RebuildIndexAsync(int z, int x, int y, CancellationToken cancellationToken = default)
        {
            RebuildRequests.Add((z, x, y));
            return Task.FromResult(_selection);
        }
    }

    private sealed class InMemoryBlobContainer
    {
        private readonly Dictionary<string, BlobState> _blobs = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Mock<BlobClient>> _clients = new(StringComparer.Ordinal);

        public Mock<BlobContainerClient> Client { get; } = new();

        public InMemoryBlobContainer()
        {
            Client
                .Setup(container => container.GetBlobClient(It.IsAny<string>()))
                .Returns((string blobName) => GetOrCreateBlobClient(blobName).Object);
        }

        public bool Contains(string blobName) => _blobs.ContainsKey(blobName);

        public string GetContent(string blobName) => _blobs[blobName].Content.ToString();

        public void SeedBlob(string blobName, BinaryData content)
        {
            _blobs[blobName] = new BlobState(content, CreateEtag(blobName, 0));
        }

        private Mock<BlobClient> GetOrCreateBlobClient(string blobName)
        {
            if (_clients.TryGetValue(blobName, out var existing))
                return existing;

            var client = new Mock<BlobClient>();

            client
                .Setup(blob => blob.DownloadContentAsync(It.IsAny<CancellationToken>()))
                .Returns((CancellationToken _) =>
                {
                    if (!_blobs.TryGetValue(blobName, out var blob))
                        throw new RequestFailedException(404, "Blob not found");

                    var details = BlobsModelFactory.BlobDownloadDetails(eTag: blob.ETag);
                    var result = BlobsModelFactory.BlobDownloadResult(blob.Content, details);
                    return Task.FromResult(Response.FromValue(result, Mock.Of<Response>()));
                });

            client
                .Setup(blob => blob.GetPropertiesAsync(
                    It.IsAny<BlobRequestConditions>(),
                    It.IsAny<CancellationToken>()))
                .Returns((BlobRequestConditions _, CancellationToken _) =>
                {
                    if (!_blobs.TryGetValue(blobName, out var blob))
                        throw new RequestFailedException(404, "Blob not found");

                    var properties = BlobsModelFactory.BlobProperties(eTag: blob.ETag);
                    return Task.FromResult(Response.FromValue(properties, Mock.Of<Response>()));
                });

            client
                .Setup(blob => blob.UploadAsync(It.IsAny<BinaryData>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns((BinaryData content, bool _, CancellationToken _) =>
                {
                    PutBlob(blobName, content);
                    return Task.FromResult(Response.FromValue(default(BlobContentInfo), Mock.Of<Response>()));
                });

            client
                .Setup(blob => blob.DeleteIfExistsAsync(
                    It.IsAny<DeleteSnapshotsOption>(),
                    It.IsAny<BlobRequestConditions?>(),
                    It.IsAny<CancellationToken>()))
                .Returns((DeleteSnapshotsOption _, BlobRequestConditions? _, CancellationToken _) =>
                {
                    var deleted = _blobs.Remove(blobName);
                    return Task.FromResult(Response.FromValue(deleted, Mock.Of<Response>()));
                });

            _clients[blobName] = client;
            return client;
        }

        private void PutBlob(string blobName, BinaryData content)
        {
            var version = _blobs.TryGetValue(blobName, out var existing) ? existing.Version + 1 : 0;
            _blobs[blobName] = new BlobState(content, CreateEtag(blobName, version), version);
        }

        private static ETag CreateEtag(string blobName, int version)
            => new($"\"{blobName}:{version}\"");

        private sealed record BlobState(BinaryData Content, ETag ETag, int Version = 0);
    }

    private static Feature CreateHighwayFeatureForTile(int z, int x, int y, string highway, string id)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, z);
        var minLng = Math.Min(southWest.Lng, northEast.Lng);
        var maxLng = Math.Max(southWest.Lng, northEast.Lng);
        var minLat = Math.Min(southWest.Lat, northEast.Lat);
        var maxLat = Math.Max(southWest.Lat, northEast.Lat);
        var centerLng = (minLng + maxLng) / 2;
        var centerLat = (minLat + maxLat) / 2;
        var deltaLng = (maxLng - minLng) / 10;
        var deltaLat = (maxLat - minLat) / 10;

        return new Feature(
            new LineString(
            [
                new Position(centerLng - deltaLng, centerLat - deltaLat),
                new Position(centerLng + deltaLng, centerLat + deltaLat)
            ]),
            new Dictionary<string, dynamic> { ["highway"] = highway },
            null,
            new FeatureId(id));
    }
}
