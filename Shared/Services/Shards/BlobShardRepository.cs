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
    Func<int, int, Shard, CancellationToken, Task>? onShardMaterialized = null) : IShardRepository
{
    private readonly BlobContainerClient _container = container;
    private readonly ILogger<BlobShardRepository> _logger = logger;
    private readonly Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> _fetchFeatures = fetchFeatures;
    private readonly int _canonicalZoom = canonicalZoom;
    private readonly int _overlapBufferMeters = overlapBufferMeters;
    private readonly Func<int, int, Shard, CancellationToken, Task>? _onShardMaterialized = onShardMaterialized;

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
                    await NotifyShardMaterializedAsync(x, y, rebuilt, cancellationToken);
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
            await NotifyShardMaterializedAsync(x, y, built, cancellationToken);
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

    public async Task DeleteShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z != _canonicalZoom)
            throw new ArgumentOutOfRangeException(nameof(z), $"Only z{_canonicalZoom} shards are supported.");

        var blob = _container.GetBlobClient(GetBlobPath(z, x, y));
        await blob.DeleteIfExistsAsync(cancellationToken: cancellationToken);
    }

    internal static string GetBlobPath(int z, int x, int y) => $"{z}/{x}/{y}.pbf";

    private async Task<Shard> BuildShardAsync(int x, int y, CancellationToken cancellationToken)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, _canonicalZoom);
        var bufferedSouthWest = GeoSpatialFunctions.ShiftCoordinate(southWest, -_overlapBufferMeters, -_overlapBufferMeters);
        var bufferedNorthEast = GeoSpatialFunctions.ShiftCoordinate(northEast, _overlapBufferMeters, _overlapBufferMeters);
        var fetched = (await _fetchFeatures(bufferedSouthWest, bufferedNorthEast, cancellationToken)).ToList();

        var clippedFeatures = BlobTileService.ClipToTileBounds(fetched, _canonicalZoom, x, y).ToList();

        // Track how many fragments per OSM ID have been seen so that multi-fragment paths
        // (an OSM way that enters/exits the tile boundary more than once) each get a unique
        // tile-scoped ID.  Without this, duplicate IDs would cause later deduplication in
        // ShardFeatureClient.GetFeaturesForShards to silently drop all but the first fragment.
        var fragmentCountByOsmId = new Dictionary<string, int>();
        var shard = new Shard();
        foreach (var feature in clippedFeatures)
        {
            var osmId = feature.Id.Value?.ToString() ?? Guid.NewGuid().ToString("N");
            fragmentCountByOsmId.TryGetValue(osmId, out var fragmentIndex);
            fragmentCountByOsmId[osmId] = fragmentIndex + 1;
            var logicalId = fragmentIndex == 0 ? osmId : $"{osmId}_f{fragmentIndex}";

            var (featureType, geometryBytes) = PackedGeometryCodec.Encode(feature.Geometry);
            shard.Owned.Add(new ShardFeature
            {
                Id = CreateTileScopedFeatureId(logicalId, x, y),
                OsmId = osmId,
                Name = feature.Properties.TryGetValue("name", out var rawName)
                    ? rawName?.ToString()
                    : null,
                Type = featureType,
                Geometry = geometryBytes,
                Tags = [.. feature.Properties
                    .Select(property => new ShardTag
                    {
                        KeyId = ShardEncodingIds.TagIdFromString(property.Key),
                        ValueId = ShardEncodingIds.TagIdFromString(property.Value?.ToString() ?? string.Empty)
                    })]
            });
        }

        _logger.LogInformation("Built shard z{Zoom}/{X}/{Y} with {Owned} clipped features.",
            _canonicalZoom, x, y, shard.Owned.Count);
        return shard;
    }

    private ulong CreateTileScopedFeatureId(string logicalId, int x, int y)
        => ShardEncodingIds.FeatureIdFromString($"{_canonicalZoom}/{x}/{y}/{logicalId}");

    private async Task NotifyShardMaterializedAsync(int x, int y, Shard shard, CancellationToken cancellationToken)
    {
        if (_onShardMaterialized is null)
            return;

        try
        {
            await _onShardMaterialized(x, y, shard, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update highway shard indexes after materializing shard z{Zoom}/{X}/{Y}.", _canonicalZoom, x, y);
        }
    }
}
