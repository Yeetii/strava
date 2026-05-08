using System.IO.Compression;
using BAMCIS.GeoJSON;

namespace Shared.Services.Shards;

public class BlobTileService(ShardFeatureClient featureClient, int shardZoom = 12)
{
    private readonly ShardFeatureClient _featureClient = featureClient;
    private readonly int _shardZoom = shardZoom;

    public async Task<byte[]> BuildTileAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        var shardKeys = GetIntersectingShardKeys(z, x, y, _shardZoom);
        var features = await _featureClient.GetFeaturesForShards(shardKeys, cancellationToken);
        var clipped = Clip(features, z, x, y);
        var pbf = MvtTileEncoder.EncodeLayer("highways", clipped, z, x, y);
        return Gzip(pbf);
    }

    internal static IReadOnlyList<(int x, int y)> GetIntersectingShardKeys(int z, int x, int y, int shardZoom)
    {
        if (z == shardZoom)
            return [(x, y)];

        if (z > shardZoom)
        {
            var scale = 1 << (z - shardZoom);
            return [(x / scale, y / scale)];
        }

        var expansion = 1 << (shardZoom - z);
        var minX = x * expansion;
        var minY = y * expansion;
        var result = new List<(int x, int y)>(expansion * expansion);
        for (var currentX = minX; currentX < minX + expansion; currentX++)
            for (var currentY = minY; currentY < minY + expansion; currentY++)
                result.Add((currentX, currentY));
        return result;
    }

    private static IEnumerable<Feature> Clip(IEnumerable<Feature> features, int z, int x, int y)
    {
        var (southWest, northEast) = Geo.SlippyTileCalculator.TileIndexToWGS84(x, y, z);
        foreach (var feature in features)
        {
            switch (feature.Geometry)
            {
                case Point point:
                    if (point.Coordinates.Longitude >= southWest.Lng
                        && point.Coordinates.Longitude <= northEast.Lng
                        && point.Coordinates.Latitude >= southWest.Lat
                        && point.Coordinates.Latitude <= northEast.Lat)
                        yield return feature;
                    break;

                case LineString line:
                    var clipped = line.Coordinates
                        .Where(position =>
                            position.Longitude >= southWest.Lng
                            && position.Longitude <= northEast.Lng
                            && position.Latitude >= southWest.Lat
                            && position.Latitude <= northEast.Lat)
                        .ToList();
                    if (clipped.Count >= 2)
                        yield return new Feature(new LineString(clipped), feature.Properties, null, feature.Id);
                    break;
            }
        }
    }

    private static byte[] Gzip(byte[] payload)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
            gzip.Write(payload, 0, payload.Length);
        return output.ToArray();
    }
}
