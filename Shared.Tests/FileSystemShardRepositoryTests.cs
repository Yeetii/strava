using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Geo;
using Shared.Models;
using Shared.Services.Shards;

namespace Shared.Tests;

/// <summary>
/// RED contract suite for the filesystem-backed shard repository (task T2).
/// These tests intentionally reference FileSystemShardRepository before it exists:
/// the build must fail until the GREEN step adds the implementation.
/// </summary>
public sealed class FileSystemShardRepositoryTests : IDisposable
{
    private const int Zoom = 12;
    private const int TileX = 2200;
    private const int TileY = 1320;

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
    public async Task TryGetShardAsync_MissingFile_ReturnsNull_WithoutFetching()
    {
        var fetchCalls = 0;
        var repository = CreateRepository((_, _, _) =>
        {
            fetchCalls++;
            return Task.FromResult<IEnumerable<Feature>>([]);
        });

        var result = await repository.TryGetShardAsync(Zoom, TileX, TileY, CancellationToken.None);

        Assert.Null(result);
        Assert.Equal(0, fetchCalls);
    }

    [Fact]
    public async Task GetShardAsync_MissingFile_BuildsPersistsAndNotifies()
    {
        var root = CreateRoot();
        var notifications = new List<(int X, int Y, Shard? Shard)>();
        var repository = CreateRepository(
            (_, _, _) => Task.FromResult<IEnumerable<Feature>>([CreateFeature(TileX, TileY, "way-1")]),
            root,
            onShardChanged: (x, y, shard, _) =>
            {
                notifications.Add((x, y, shard));
                return Task.CompletedTask;
            });

        var shard = await repository.GetShardAsync(Zoom, TileX, TileY, CancellationToken.None);

        Assert.Single(shard.Owned);
        Assert.Equal("way-1", shard.Owned[0].OsmId);
        var path = FileSystemShardRepository.GetShardPath(Zoom, TileX, TileY);
        Assert.True(File.Exists(Path.Combine(root, path)));
        var persisted = ShardBinarySerializer.Deserialize(File.ReadAllBytes(Path.Combine(root, path)));
        Assert.Equal("way-1", Assert.Single(persisted.Owned).OsmId);
        Assert.Single(notifications);
        Assert.Equal((TileX, TileY), (notifications[0].X, notifications[0].Y));
        Assert.Single(notifications[0].Shard!.Owned);
    }

    [Theory]
    [InlineData(11)]
    [InlineData(13)]
    public async Task GetShardAsync_NonCanonicalZoom_Throws(int zoom)
    {
        var repository = CreateRepository((_, _, _) => Task.FromResult<IEnumerable<Feature>>([]));

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => repository.GetShardAsync(zoom, TileX, TileY, CancellationToken.None));
    }

    [Fact]
    public async Task GetShardAsync_EmptyCachedShard_RevalidatesWithFetchAndOverwrites()
    {
        var root = CreateRoot();
        var path = FileSystemShardRepository.GetShardPath(Zoom, TileX, TileY);
        SeedShard(root, path, new Shard());
        var fetchCalls = 0;
        var repository = CreateRepository(
            (_, _, _) =>
            {
                fetchCalls++;
                return Task.FromResult<IEnumerable<Feature>>([CreateFeature(TileX, TileY, "way-revalidated")]);
            },
            root);

        var shard = await repository.GetShardAsync(Zoom, TileX, TileY, CancellationToken.None);

        Assert.Equal(1, fetchCalls);
        Assert.Equal("way-revalidated", Assert.Single(shard.Owned).OsmId);
        var persisted = ShardBinarySerializer.Deserialize(File.ReadAllBytes(Path.Combine(root, path)));
        Assert.Equal("way-revalidated", Assert.Single(persisted.Owned).OsmId);
    }

    [Fact]
    public async Task TryGetShardLastModifiedAsync_ReturnsFileMtime_OrNullWhenMissing()
    {
        var root = CreateRoot();
        var path = FileSystemShardRepository.GetShardPath(Zoom, TileX, TileY);
        var repository = CreateRepository((_, _, _) => Task.FromResult<IEnumerable<Feature>>([]), root);

        Assert.Null(await repository.TryGetShardLastModifiedAsync(Zoom, TileX, TileY, CancellationToken.None));

        SeedShard(root, path, new Shard());
        var before = DateTimeOffset.UtcNow.AddSeconds(-2);
        var actual = await repository.TryGetShardLastModifiedAsync(Zoom, TileX, TileY, CancellationToken.None);
        var after = DateTimeOffset.UtcNow.AddSeconds(2);

        Assert.NotNull(actual);
        Assert.InRange(actual.Value, before, after);
    }

    [Fact]
    public async Task RebuildShardAsync_OverwritesFile_AndNotifies()
    {
        var root = CreateRoot();
        var path = FileSystemShardRepository.GetShardPath(Zoom, TileX, TileY);
        SeedShard(root, path, CreateShardWithOsmId("stale"));
        var notifications = new List<Shard?>();
        var repository = CreateRepository(
            (_, _, _) => Task.FromResult<IEnumerable<Feature>>([CreateFeature(TileX, TileY, "fresh")]),
            root,
            onShardChanged: (_, _, shard, _) =>
            {
                notifications.Add(shard);
                return Task.CompletedTask;
            });

        var rebuilt = await repository.RebuildShardAsync(Zoom, TileX, TileY, CancellationToken.None);

        Assert.Equal("fresh", Assert.Single(rebuilt.Owned).OsmId);
        var persisted = ShardBinarySerializer.Deserialize(File.ReadAllBytes(Path.Combine(root, path)));
        Assert.Equal("fresh", Assert.Single(persisted.Owned).OsmId);
        Assert.Single(notifications);
        Assert.Equal("fresh", Assert.Single(notifications[0]!.Owned).OsmId);
    }

    [Fact]
    public async Task DeleteShardAsync_ExistingFile_DeletesAndNotifiesWithNull()
    {
        var root = CreateRoot();
        var path = FileSystemShardRepository.GetShardPath(Zoom, TileX, TileY);
        SeedShard(root, path, new Shard());
        var notifications = new List<Shard?>();
        var repository = CreateRepository(
            (_, _, _) => Task.FromResult<IEnumerable<Feature>>([]),
            root,
            onShardChanged: (_, _, shard, _) =>
            {
                notifications.Add(shard);
                return Task.CompletedTask;
            });

        await repository.DeleteShardAsync(Zoom, TileX, TileY, CancellationToken.None);

        Assert.False(File.Exists(Path.Combine(root, path)));
        Assert.Single(notifications);
        Assert.Null(notifications[0]);
    }

    [Fact]
    public async Task DeleteShardAsync_MissingFile_DoesNotNotify()
    {
        var notifications = new List<Shard?>();
        var repository = CreateRepository(
            (_, _, _) => Task.FromResult<IEnumerable<Feature>>([]),
            onShardChanged: (_, _, shard, _) =>
            {
                notifications.Add(shard);
                return Task.CompletedTask;
            });

        await repository.DeleteShardAsync(Zoom, TileX, TileY, CancellationToken.None);

        Assert.Empty(notifications);
    }

    [Fact]
    public void GetShardPath_MirrorsBlobLayout_AndStaysUnderRoot()
    {
        var relative = FileSystemShardRepository.GetShardPath(Zoom, TileX, TileY);

        var root = CreateRoot();
        Assert.Equal($"{Zoom}/{TileX}/{TileY}.pbf", relative);
        Assert.False(Path.IsPathRooted(relative));
        var full = Path.GetFullPath(Path.Combine(root, relative));
        Assert.Equal(Path.GetFullPath(root), Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(full))));
    }

    private string CreateRoot()
    {
        var root = Path.Combine(Path.GetTempPath(), "strava-shard-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        _roots.Add(root);
        return root;
    }

    private static void SeedShard(string root, string relativePath, Shard shard)
    {
        var full = Path.Combine(root, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(full)!);
        File.WriteAllBytes(full, ShardBinarySerializer.Serialize(shard));
    }

    private static Shard CreateShardWithOsmId(string id)
    {
        var shard = new Shard();
        shard.Owned.Add(new ShardFeature
        {
            Id = ShardEncodingIds.FeatureIdFromString(id),
            OsmId = id,
            Type = ShardFeatureType.LineString,
            Geometry = []
        });
        return shard;
    }

    private IShardRepository CreateRepository(
        Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> fetchFeatures,
        string? root = null,
        Func<int, int, Shard?, CancellationToken, Task>? onShardChanged = null)
    {
        return new FileSystemShardRepository(
            root ?? CreateRoot(),
            NullLogger<FileSystemShardRepository>.Instance,
            fetchFeatures,
            canonicalZoom: Zoom,
            overlapBufferMeters: 200,
            onShardChanged: onShardChanged);
    }

    private static Feature CreateFeature(int x, int y, string id)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, Zoom);
        var centerLng = (southWest.Lng + northEast.Lng) / 2;
        var centerLat = (southWest.Lat + northEast.Lat) / 2;
        var deltaLng = Math.Abs(northEast.Lng - southWest.Lng) / 10;
        var deltaLat = Math.Abs(northEast.Lat - southWest.Lat) / 10;

        return new Feature(
            new LineString(
            [
                new Position(centerLng - deltaLng, centerLat - deltaLat),
                new Position(centerLng + deltaLng, centerLat + deltaLat)
            ]),
            new Dictionary<string, dynamic> { ["highway"] = "track" },
            null,
            new FeatureId(id));
    }
}
