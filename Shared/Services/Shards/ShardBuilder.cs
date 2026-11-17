using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services.Shards;

/// <summary>
/// The storage-agnostic part of shard construction: fetch from Overpass, clip to the
/// canonical tile, and encode the packed shard payload. Shared by the Blob and
/// filesystem repositories so the two stores cannot drift in how a shard is built.
/// </summary>
internal static class ShardBuilder
{
    internal static async Task<Shard> BuildAsync(
        int x,
        int y,
        int canonicalZoom,
        int overlapBufferMeters,
        Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> fetchFeatures,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, canonicalZoom);
        var bufferedSouthWest = GeoSpatialFunctions.ShiftCoordinate(southWest, -overlapBufferMeters, -overlapBufferMeters);
        var bufferedNorthEast = GeoSpatialFunctions.ShiftCoordinate(northEast, overlapBufferMeters, overlapBufferMeters);
        var fetched = (await fetchFeatures(bufferedSouthWest, bufferedNorthEast, cancellationToken)).ToList();

        var clippedFeatures = BlobTileService.ClipToTileBounds(fetched, canonicalZoom, x, y).ToList();

        // A way that enters/exits the tile more than once yields several fragments;
        // each gets a unique tile-scoped ID so later deduplication does not drop them.
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
                Id = CreateTileScopedFeatureId(logicalId, x, y, canonicalZoom),
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

        logger.LogInformation("Built shard z{Zoom}/{X}/{Y} with {Owned} clipped features.",
            canonicalZoom, x, y, shard.Owned.Count);
        return shard;
    }

    private static ulong CreateTileScopedFeatureId(string logicalId, int x, int y, int canonicalZoom)
        => ShardEncodingIds.FeatureIdFromString($"{canonicalZoom}/{x}/{y}/{logicalId}");
}
