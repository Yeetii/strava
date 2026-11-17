using Azure;
using Azure.Storage.Blobs;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services.Shards;

public class BlobShardRepository(
    BlobContainerClient container,
    ILogger<BlobShardRepository> logger,
    Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> fetchFeatures,
    int canonicalZoom = 12,
    int overlapBufferMeters = 200,
    Func<int, int, Shard?, CancellationToken, Task>? onShardChanged = null) : IShardRepository
{
    private readonly BlobContainerClient _container = container;
    private readonly ILogger<BlobShardRepository> _logger = logger;
    private readonly Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> _fetchFeatures = fetchFeatures;
    private readonly int _canonicalZoom = canonicalZoom;
    private readonly int _overlapBufferMeters = overlapBufferMeters;
    private readonly Func<int, int, Shard?, CancellationToken, Task>? _onShardChanged = onShardChanged;

    public int CanonicalZoom => _canonicalZoom;

    public async Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z != _canonicalZoom)
            throw new ArgumentOutOfRangeException(nameof(z), $"Only z{_canonicalZoom} shards are supported.");

        var blob = _container.GetBlobClient(GetBlobPath(z, x, y));
        try
        {
            var download = await blob.DownloadContentAsync(cancellationToken);
            var cached = ShardBinarySerializer.Deserialize(download.Value.Content.ToArray());

            // A previously failed Overpass fetch may have written an empty shard to blob.
            // Re-validate by querying Overpass again; if it returns features we overwrite the bad cache entry.
            // If Overpass also confirms empty (2-mirror corroboration), we trust the cached empty shard —
            // the in-memory LRU cache in ShardFeatureClient prevents repeated Overpass calls within a process.
            if (cached.Owned.Count == 0)
            {
                _logger.LogInformation("Cached shard z{Zoom}/{X}/{Y} has 0 features; re-fetching from Overpass to verify.", z, x, y);
                var rebuilt = await BuildShardAsync(x, y, cancellationToken);
                if (rebuilt.Owned.Count > 0)
                {
                    var bytes = ShardBinarySerializer.Serialize(rebuilt);
                    await blob.UploadAsync(BinaryData.FromBytes(bytes), overwrite: true, cancellationToken);
                    await NotifyShardChangedAsync(x, y, rebuilt, cancellationToken);
                    _logger.LogInformation("Replaced empty shard z{Zoom}/{X}/{Y} with {Count} features.", z, x, y, rebuilt.Owned.Count);
                    return rebuilt;
                }
            }

            return cached;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            var built = await BuildShardAsync(x, y, cancellationToken);
            var bytes = ShardBinarySerializer.Serialize(built);
            await blob.UploadAsync(BinaryData.FromBytes(bytes), overwrite: true, cancellationToken);
            await NotifyShardChangedAsync(x, y, built, cancellationToken);
            return built;
        }
    }

    public async Task<Shard?> TryGetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z != _canonicalZoom)
            throw new ArgumentOutOfRangeException(nameof(z), $"Only z{_canonicalZoom} shards are supported.");

        var blob = _container.GetBlobClient(GetBlobPath(z, x, y));
        try
        {
            var download = await blob.DownloadContentAsync(cancellationToken);
            return ShardBinarySerializer.Deserialize(download.Value.Content.ToArray());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<DateTimeOffset?> TryGetShardLastModifiedAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z != _canonicalZoom)
            throw new ArgumentOutOfRangeException(nameof(z), $"Only z{_canonicalZoom} shards are supported.");

        var blob = _container.GetBlobClient(GetBlobPath(z, x, y));
        try
        {
            var properties = await blob.GetPropertiesAsync(cancellationToken: cancellationToken);
            return properties.Value.LastModified;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<Shard> RebuildShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z != _canonicalZoom)
            throw new ArgumentOutOfRangeException(nameof(z), $"Only z{_canonicalZoom} shards are supported.");

        var blob = _container.GetBlobClient(GetBlobPath(z, x, y));
        var rebuilt = await BuildShardAsync(x, y, cancellationToken);
        var bytes = ShardBinarySerializer.Serialize(rebuilt);
        await blob.UploadAsync(BinaryData.FromBytes(bytes), overwrite: true, cancellationToken);
        await NotifyShardChangedAsync(x, y, rebuilt, cancellationToken);
        return rebuilt;
    }

    public async Task DeleteShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z != _canonicalZoom)
            throw new ArgumentOutOfRangeException(nameof(z), $"Only z{_canonicalZoom} shards are supported.");

        var blob = _container.GetBlobClient(GetBlobPath(z, x, y));
        var deleted = await blob.DeleteIfExistsAsync(cancellationToken: cancellationToken);
        if (deleted.Value)
            await NotifyShardChangedAsync(x, y, shard: null, cancellationToken);
    }

    internal static string GetBlobPath(int z, int x, int y) => $"{z}/{x}/{y}.pbf";

    private Task<Shard> BuildShardAsync(int x, int y, CancellationToken cancellationToken)
        => ShardBuilder.BuildAsync(x, y, _canonicalZoom, _overlapBufferMeters, _fetchFeatures, _logger, cancellationToken);

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
