using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Models;
using Shared.Services.Shards;

namespace Shared.Tests;

/// <summary>
/// RED contract suite for T3: storage-agnostic zoom-index persistence. References
/// IShardIndexStore / FileSystemShardIndexStore and the new HighwayZoomIndexService
/// constructor before they exist, so the build must fail until GREEN.
/// </summary>
public sealed class ShardIndexStoreTests : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private const int Zoom = 12;
    private readonly List<string> _roots = [];

    public void Dispose()
    {
        foreach (var root in _roots)
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task FileSystemShardIndexStore_TryReadAsync_MissingFile_ReturnsNull()
    {
        var store = new FileSystemShardIndexStore(CreateRoot());

        Assert.Null(await store.TryReadAsync("index/7/68/38.json", CancellationToken.None));
    }

    [Fact]
    public async Task FileSystemShardIndexStore_WriteThenRead_RoundTripsAtExpectedPath()
    {
        var root = CreateRoot();
        var store = new FileSystemShardIndexStore(root);
        var payload = new byte[] { 1, 2, 3, 4 };

        await store.WriteAsync("index/7/68/38.json", payload, CancellationToken.None);

        Assert.Equal(payload, await store.TryReadAsync("index/7/68/38.json", CancellationToken.None));
        Assert.True(File.Exists(Path.Combine(root, "index/7/68/38.json")));
        Assert.Empty(Directory.GetFiles(root, "*.tmp", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task FileSystemShardIndexStore_TryGetLastModifiedAsync_Missing_ReturnsNull()
    {
        var root = CreateRoot();
        var store = new FileSystemShardIndexStore(root);

        Assert.Null(await store.TryGetLastModifiedAsync("index/7/68/38.json", CancellationToken.None));

        await store.WriteAsync("index/7/68/38.json", [1], CancellationToken.None);
        var before = DateTimeOffset.UtcNow.AddSeconds(-2);
        var actual = await store.TryGetLastModifiedAsync("index/7/68/38.json", CancellationToken.None);
        var after = DateTimeOffset.UtcNow.AddSeconds(2);

        Assert.NotNull(actual);
        Assert.InRange(actual.Value, before, after);
    }

    [Fact]
    public async Task FileSystemShardIndexStore_RejectsPathOutsideRoot()
    {
        var store = new FileSystemShardIndexStore(CreateRoot());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.WriteAsync("../escape.json", [1], CancellationToken.None));
    }

    [Fact]
    public async Task HighwayZoomIndexService_RebuildIndexAsync_PersistsVersion4IndexThroughStore()
    {
        var store = new InMemoryShardIndexStore();
        var shard = CreateShardWithMajorRoad();
        var service = new HighwayZoomIndexService(
            store,
            new FakeShardRepository(shard),
            NullLogger<HighwayZoomIndexService>.Instance,
            canonicalZoom: Zoom);

        var selection = await service.RebuildIndexAsync(7, 68, 38, CancellationToken.None);

        var path = HighwayZoomIndexService.GetIndexBlobPath(7, 68, 38);
        Assert.True(store.Entries.ContainsKey(path));
        var index = JsonSerializer.Deserialize<HighwayTileShardIndex>(store.Entries[path], JsonOptions);
        Assert.NotNull(index);
        Assert.Equal(4, index.Version);
        Assert.Equal(7, index.Zoom);
        Assert.NotEmpty(index.Shards);
        Assert.Equal(index.Shards.Count, selection.Shards.Count);
    }

    [Fact]
    public async Task HighwayZoomIndexService_TryGetIndexLastModifiedAsync_ReadsFromStore()
    {
        var store = new InMemoryShardIndexStore();
        var timestamp = new DateTimeOffset(2026, 11, 17, 12, 0, 0, TimeSpan.Zero);
        store.SetLastModified(HighwayZoomIndexService.GetIndexBlobPath(7, 68, 38), timestamp);
        var service = new HighwayZoomIndexService(
            store,
            new FakeShardRepository(null),
            NullLogger<HighwayZoomIndexService>.Instance,
            canonicalZoom: Zoom);

        var actual = await service.TryGetIndexLastModifiedAsync(7, 68, 38, CancellationToken.None);

        Assert.Equal(timestamp, actual);
    }

    private string CreateRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), "strava-index-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _roots.Add(root);
        return root;
    }

    private static Shard CreateShardWithMajorRoad()
    {
        var shard = new Shard();
        shard.Owned.Add(new ShardFeature
        {
            Id = ShardEncodingIds.FeatureIdFromString("way-1"),
            OsmId = "way-1",
            Type = ShardFeatureType.LineString,
            Geometry = [],
            Tags = [new ShardTag { KeyId = ShardEncodingIds.TagIdFromString("highway"), ValueId = ShardEncodingIds.TagIdFromString("primary") }]
        });
        return shard;
    }

    private sealed class InMemoryShardIndexStore : IShardIndexStore
    {
        public Dictionary<string, byte[]> Entries { get; } = [];
        private Dictionary<string, DateTimeOffset> LastModified { get; } = [];

        public void SetLastModified(string path, DateTimeOffset value) => LastModified[path] = value;

        public Task<byte[]?> TryReadAsync(string path, CancellationToken cancellationToken = default)
            => Task.FromResult(Entries.TryGetValue(path, out var value) ? value : null);

        public Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default)
        {
            Entries[path] = content;
            LastModified[path] = DateTimeOffset.UtcNow;
            return Task.CompletedTask;
        }

        public Task<DateTimeOffset?> TryGetLastModifiedAsync(string path, CancellationToken cancellationToken = default)
            => Task.FromResult(LastModified.TryGetValue(path, out var value) ? value : (DateTimeOffset?)null);
    }

    private sealed class FakeShardRepository(Shard? shard) : IShardRepository
    {
        public Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
            => Task.FromResult(shard ?? new Shard());

        public Task<Shard?> TryGetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
            => Task.FromResult(shard);

        public Task<DateTimeOffset?> TryGetShardLastModifiedAsync(int z, int x, int y, CancellationToken cancellationToken = default)
            => Task.FromResult<DateTimeOffset?>(null);

        public Task<Shard> RebuildShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
            => Task.FromResult(shard ?? new Shard());

        public Task DeleteShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
            => Task.CompletedTask;
    }
}
