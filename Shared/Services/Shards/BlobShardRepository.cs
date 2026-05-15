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
    int overlapBufferMeters = 200) : IShardRepository
{
    private readonly BlobContainerClient _container = container;
    private readonly ILogger<BlobShardRepository> _logger = logger;
    private readonly Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> _fetchFeatures = fetchFeatures;
    private readonly int _canonicalZoom = canonicalZoom;
    private readonly int _overlapBufferMeters = overlapBufferMeters;

    public int CanonicalZoom => _canonicalZoom;

    public async Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default)
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
            var built = await BuildShardAsync(x, y, cancellationToken);
            var bytes = ShardBinarySerializer.Serialize(built);
            await blob.UploadAsync(BinaryData.FromBytes(bytes), overwrite: true, cancellationToken);
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

        var shard = new Shard();
        foreach (var feature in clippedFeatures)
        {
            var logicalId = feature.Id.Value?.ToString() ?? Guid.NewGuid().ToString("N");
            var (featureType, geometryBytes) = PackedGeometryCodec.Encode(feature.Geometry);
            shard.Owned.Add(new ShardFeature
            {
                Id = CreateTileScopedFeatureId(logicalId, x, y),
                OsmId = logicalId,
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
}
