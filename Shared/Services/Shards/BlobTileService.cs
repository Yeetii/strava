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
                    var clipped = ClipLineToBounds(line, southWest.Lng, southWest.Lat, northEast.Lng, northEast.Lat);
                    if (clipped.Count >= 2)
                        yield return new Feature(new LineString(clipped), feature.Properties, null, feature.Id);
                    break;
            }
        }
    }

    private static List<Position> ClipLineToBounds(LineString line, double minX, double minY, double maxX, double maxY)
    {
        var clipped = new List<Position>();
        Position? lastPoint = null;
        foreach (var segment in line.Coordinates.Zip(line.Coordinates.Skip(1)))
        {
            if (!TryClipSegment(segment.First, segment.Second, minX, minY, maxX, maxY, out var start, out var end))
                continue;

            if (lastPoint is null || lastPoint.Longitude != start.Longitude || lastPoint.Latitude != start.Latitude)
                clipped.Add(start);
            clipped.Add(end);
            lastPoint = end;
        }

        return clipped;
    }

    private static bool TryClipSegment(Position p0, Position p1, double minX, double minY, double maxX, double maxY, out Position c0, out Position c1)
    {
        var x0 = p0.Longitude;
        var y0 = p0.Latitude;
        var x1 = p1.Longitude;
        var y1 = p1.Latitude;
        var code0 = ComputeCode(x0, y0, minX, minY, maxX, maxY);
        var code1 = ComputeCode(x1, y1, minX, minY, maxX, maxY);

        while (true)
        {
            if ((code0 | code1) == 0)
            {
                c0 = new Position(x0, y0);
                c1 = new Position(x1, y1);
                return true;
            }

            if ((code0 & code1) != 0)
            {
                c0 = default!;
                c1 = default!;
                return false;
            }

            var outCode = code0 != 0 ? code0 : code1;
            double x;
            double y;
            if ((outCode & 8) != 0)
            {
                if (Math.Abs(y1 - y0) < double.Epsilon)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                x = x0 + ((x1 - x0) * (maxY - y0) / (y1 - y0));
                y = maxY;
            }
            else if ((outCode & 4) != 0)
            {
                if (Math.Abs(y1 - y0) < double.Epsilon)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                x = x0 + ((x1 - x0) * (minY - y0) / (y1 - y0));
                y = minY;
            }
            else if ((outCode & 2) != 0)
            {
                if (Math.Abs(x1 - x0) < double.Epsilon)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                y = y0 + ((y1 - y0) * (maxX - x0) / (x1 - x0));
                x = maxX;
            }
            else
            {
                if (Math.Abs(x1 - x0) < double.Epsilon)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                y = y0 + ((y1 - y0) * (minX - x0) / (x1 - x0));
                x = minX;
            }

            if (outCode == code0)
            {
                x0 = x;
                y0 = y;
                code0 = ComputeCode(x0, y0, minX, minY, maxX, maxY);
            }
            else
            {
                x1 = x;
                y1 = y;
                code1 = ComputeCode(x1, y1, minX, minY, maxX, maxY);
            }
        }
    }

    private static int ComputeCode(double x, double y, double minX, double minY, double maxX, double maxY)
    {
        var code = 0;
        if (x < minX) code |= 1;
        else if (x > maxX) code |= 2;
        if (y < minY) code |= 4;
        else if (y > maxY) code |= 8;
        return code;
    }

    private static byte[] Gzip(byte[] payload)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
            gzip.Write(payload, 0, payload.Length);
        return output.ToArray();
    }
}
