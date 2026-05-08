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

    internal static string GetBlobPath(int z, int x, int y) => $"{z}/{x}/{y}.pbf";

    private async Task<Shard> BuildShardAsync(int x, int y, CancellationToken cancellationToken)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, _canonicalZoom);
        var bufferedSouthWest = GeoSpatialFunctions.ShiftCoordinate(southWest, -_overlapBufferMeters, -_overlapBufferMeters);
        var bufferedNorthEast = GeoSpatialFunctions.ShiftCoordinate(northEast, _overlapBufferMeters, _overlapBufferMeters);
        var fetched = (await _fetchFeatures(bufferedSouthWest, bufferedNorthEast, cancellationToken)).ToList();

        var featuresByOwner = fetched
            .Select(feature =>
            {
                var id = feature.Id.Value?.ToString() ?? string.Empty;
                var centroid = GeometryCentroidHelper.GetCentroid(feature.Geometry);
                var owner = SlippyTileCalculator.WGS84ToTileIndex(centroid, _canonicalZoom);
                var touches = FeatureTouchesTiles(feature.Geometry, _canonicalZoom);
                return new CandidateFeature(id, owner.x, owner.y, feature, touches);
            })
            .Where(candidate => candidate.TouchingTiles.Contains((x, y)))
            .ToList();

        var ownerLocalIndex = featuresByOwner
            .GroupBy(candidate => (candidate.OwnerX, candidate.OwnerY))
            .ToDictionary(
                group => group.Key,
                group => group.OrderBy(candidate => candidate.LogicalId, StringComparer.Ordinal)
                    .Select((candidate, index) => new KeyValuePair<string, uint>(candidate.LogicalId, (uint)index))
                    .ToDictionary(item => item.Key, item => item.Value, StringComparer.Ordinal));

        var shard = new Shard();
        foreach (var candidate in featuresByOwner)
        {
            var featureId = ShardEncodingIds.FeatureIdFromString(candidate.LogicalId);
            if (candidate.OwnerX == x && candidate.OwnerY == y)
            {
                var (featureType, geometryBytes) = PackedGeometryCodec.Encode(candidate.Feature.Geometry);
                shard.Owned.Add(new ShardFeature
                {
                    Id = featureId,
                    Type = featureType,
                    Geometry = geometryBytes,
                    Tags = candidate.Feature.Properties
                        .Select(property => new ShardTag
                        {
                            KeyId = ShardEncodingIds.TagIdFromString(property.Key),
                            ValueId = ShardEncodingIds.TagIdFromString(property.Value?.ToString() ?? string.Empty)
                        })
                        .ToList()
                });
                continue;
            }

            var localIndex = ownerLocalIndex.TryGetValue((candidate.OwnerX, candidate.OwnerY), out var localIndices)
                && localIndices.TryGetValue(candidate.LogicalId, out var found)
                ? found
                : uint.MaxValue;

            shard.Pointers.Add(new FeaturePointer
            {
                FeatureId = featureId,
                OwnerX = (uint)candidate.OwnerX,
                OwnerY = (uint)candidate.OwnerY,
                LocalIndex = localIndex
            });
        }

        _logger.LogInformation("Built shard z{Zoom}/{X}/{Y} with {Owned} owned and {Pointers} pointers.",
            _canonicalZoom, x, y, shard.Owned.Count, shard.Pointers.Count);
        return shard;
    }

    private static HashSet<(int x, int y)> FeatureTouchesTiles(Geometry geometry, int zoom)
    {
        return geometry switch
        {
            Point point => [SlippyTileCalculator.WGS84ToTileIndex(new Coordinate(point.Coordinates.Longitude, point.Coordinates.Latitude), zoom)],
            LineString line => [.. line.Coordinates.Select(position => SlippyTileCalculator.WGS84ToTileIndex(new Coordinate(position.Longitude, position.Latitude), zoom))],
            _ => []
        };
    }

    private sealed record CandidateFeature(string LogicalId, int OwnerX, int OwnerY, Feature Feature, HashSet<(int x, int y)> TouchingTiles);
}
