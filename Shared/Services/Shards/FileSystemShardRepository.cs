using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services.Shards;

/// <summary>
/// Local-filesystem implementation of <see cref="IShardRepository"/>. Layout mirrors
/// the Blob container exactly ({z}/{x}/{y}.pbf) so a shard export can be copied in
/// without transformation, and the same <see cref="ShardBinarySerializer"/> format is
/// used on both stores.
/// </summary>
public class FileSystemShardRepository(
    string rootDir,
    ILogger<FileSystemShardRepository> logger,
    Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> fetchFeatures,
    int canonicalZoom = 12,
    int overlapBufferMeters = 200,
    Func<int, int, Shard?, CancellationToken, Task>? onShardChanged = null) : IShardRepository
{
    private readonly LocalShardFiles _files = new(rootDir);
    private readonly ILogger _logger = logger;
    private readonly Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> _fetchFeatures = fetchFeatures;
    private readonly int _canonicalZoom = canonicalZoom;
    private readonly int _overlapBufferMeters = overlapBufferMeters;
    private readonly Func<int, int, Shard?, CancellationToken, Task>? _onShardChanged = onShardChanged;

    public int CanonicalZoom => _canonicalZoom;

    public async Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        EnsureCanonicalZoom(z);
        var path = ResolvePath(z, x, y);
        if (!File.Exists(path))
        {
            var built = await BuildShardAsync(x, y, cancellationToken);
            await PersistAsync(path, built, cancellationToken);
            await NotifyShardChangedAsync(x, y, built, cancellationToken);
            return built;
        }

        var cached = ShardBinarySerializer.Deserialize(await File.ReadAllBytesAsync(path, cancellationToken));

        // A previously failed Overpass fetch may have written an empty shard to disk.
        // Re-validate the same way the Blob repository does; if Overpass also confirms
        // empty, keep the cached empty shard so the LRU in ShardFeatureClient still wins.
        if (cached.Owned.Count == 0)
        {
            _logger.LogInformation("Cached shard z{Zoom}/{X}/{Y} has 0 features; re-fetching from Overpass to verify.", z, x, y);
            var rebuilt = await BuildShardAsync(x, y, cancellationToken);
            if (rebuilt.Owned.Count > 0)
            {
                await PersistAsync(path, rebuilt, cancellationToken);
                await NotifyShardChangedAsync(x, y, rebuilt, cancellationToken);
                _logger.LogInformation("Replaced empty shard z{Zoom}/{X}/{Y} with {Count} features.", z, x, y, rebuilt.Owned.Count);
                return rebuilt;
            }
        }

        return cached;
    }

    public async Task<Shard?> TryGetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        EnsureCanonicalZoom(z);
        var path = ResolvePath(z, x, y);
        if (!File.Exists(path))
            return null;

        return ShardBinarySerializer.Deserialize(await File.ReadAllBytesAsync(path, cancellationToken));
    }

    public Task<DateTimeOffset?> TryGetShardLastModifiedAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        EnsureCanonicalZoom(z);
        var path = ResolvePath(z, x, y);
        return Task.FromResult<DateTimeOffset?>(
            File.Exists(path) ? new DateTimeOffset(File.GetLastWriteTimeUtc(path)) : null);
    }

    public async Task<Shard> RebuildShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        EnsureCanonicalZoom(z);
        var path = ResolvePath(z, x, y);
        var rebuilt = await BuildShardAsync(x, y, cancellationToken);
        await PersistAsync(path, rebuilt, cancellationToken);
        await NotifyShardChangedAsync(x, y, rebuilt, cancellationToken);
        return rebuilt;
    }

    public async Task DeleteShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        EnsureCanonicalZoom(z);
        var path = ResolvePath(z, x, y);
        if (!File.Exists(path))
            return;

        File.Delete(path);
        await NotifyShardChangedAsync(x, y, shard: null, cancellationToken);
    }

    internal static string GetShardPath(int z, int x, int y) => $"{z}/{x}/{y}.pbf";

    private void EnsureCanonicalZoom(int z)
    {
        if (z != _canonicalZoom)
            throw new ArgumentOutOfRangeException(nameof(z), $"Only z{_canonicalZoom} shards are supported.");
    }

    private string ResolvePath(int z, int x, int y) => _files.Resolve(GetShardPath(z, x, y));

    private Task<Shard> BuildShardAsync(int x, int y, CancellationToken cancellationToken)
        => ShardBuilder.BuildAsync(x, y, _canonicalZoom, _overlapBufferMeters, _fetchFeatures, _logger, cancellationToken);

    private Task PersistAsync(string path, Shard shard, CancellationToken cancellationToken)
        => _files.WriteAtomicAsync(path, ShardBinarySerializer.Serialize(shard), cancellationToken);

    private async Task NotifyShardChangedAsync(int x, int y, Shard? shard, CancellationToken cancellationToken)
    {
        if (_onShardChanged is null)
            return;

        try
        {
            await _onShardChanged(x, y, shard, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to sync highway shard indexes after shard change z{Zoom}/{X}/{Y}.", _canonicalZoom, x, y);
        }
    }
}
