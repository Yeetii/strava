using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services.Shards;

public static class MvtTileEncoder
{
    private const int Extent = 4096;

    public static byte[] EncodeLayer(string layerName, IEnumerable<Feature> features, int z, int x, int y)
    {
        using var tileStream = new MemoryStream();
        ProtoWire.WriteTag(tileStream, 3, 2); // layers
        var layerBytes = BuildLayer(layerName, features, z, x, y);
        ProtoWire.WriteLengthDelimited(tileStream, layerBytes);
        return tileStream.ToArray();
    }

    private static byte[] BuildLayer(string layerName, IEnumerable<Feature> features, int z, int x, int y)
    {
        using var layerStream = new MemoryStream();
        ProtoWire.WriteTag(layerStream, 15, 0); // version
        ProtoWire.WriteVarint(layerStream, 2);
        ProtoWire.WriteTag(layerStream, 1, 2); // name
        ProtoWire.WriteLengthDelimited(layerStream, System.Text.Encoding.UTF8.GetBytes(layerName));
        ProtoWire.WriteTag(layerStream, 5, 0); // extent
        ProtoWire.WriteVarint(layerStream, Extent);

        foreach (var feature in features)
        {
            var encoded = EncodeFeature(feature, z, x, y);
            if (encoded.Length == 0)
                continue;
            ProtoWire.WriteTag(layerStream, 2, 2);
            ProtoWire.WriteLengthDelimited(layerStream, encoded);
        }

        return layerStream.ToArray();
    }

    private static byte[] EncodeFeature(Feature feature, int z, int x, int y)
    {
        var geometryType = feature.Geometry switch
        {
            Point => 1u,
            LineString => 2u,
            _ => 0u
        };

        if (geometryType == 0)
            return [];

        var commands = feature.Geometry switch
        {
            Point point => EncodePoint(point, z, x, y),
            LineString line => EncodeLine(line, z, x, y),
            _ => []
        };

        if (commands.Length == 0)
            return [];

        using var stream = new MemoryStream();
        var id = ulong.TryParse(feature.Id.Value?.ToString(), out var parsedId)
            ? parsedId
            : ShardEncodingIds.FeatureIdFromString(feature.Id.Value?.ToString() ?? Guid.NewGuid().ToString("N"));
        ProtoWire.WriteTag(stream, 1, 0);
        ProtoWire.WriteVarint(stream, id);
        ProtoWire.WriteTag(stream, 3, 0);
        ProtoWire.WriteVarint(stream, geometryType);
        ProtoWire.WriteTag(stream, 4, 2);
        ProtoWire.WriteLengthDelimited(stream, commands);
        return stream.ToArray();
    }

    private static byte[] EncodePoint(Point point, int z, int x, int y)
    {
        var projected = Project(point.Coordinates.Longitude, point.Coordinates.Latitude, z, x, y);
        if (projected is null)
            return [];

        using var stream = new MemoryStream();
        ProtoWire.WriteVarint(stream, CommandInteger(1, 1)); // MoveTo
        ProtoWire.WriteVarint(stream, ZigZag(projected.Value.x));
        ProtoWire.WriteVarint(stream, ZigZag(projected.Value.y));
        return stream.ToArray();
    }

    private static byte[] EncodeLine(LineString line, int z, int x, int y)
    {
        var projected = line.Coordinates
            .Select(position => Project(position.Longitude, position.Latitude, z, x, y))
            .Where(point => point.HasValue)
            .Select(point => point!.Value)
            .ToList();

        if (projected.Count < 2)
            return [];

        using var stream = new MemoryStream();
        ProtoWire.WriteVarint(stream, CommandInteger(1, 1)); // MoveTo
        ProtoWire.WriteVarint(stream, ZigZag(projected[0].x));
        ProtoWire.WriteVarint(stream, ZigZag(projected[0].y));

        ProtoWire.WriteVarint(stream, CommandInteger(2, projected.Count - 1)); // LineTo
        var prev = projected[0];
        for (var i = 1; i < projected.Count; i++)
        {
            var current = projected[i];
            ProtoWire.WriteVarint(stream, ZigZag(current.x - prev.x));
            ProtoWire.WriteVarint(stream, ZigZag(current.y - prev.y));
            prev = current;
        }

        return stream.ToArray();
    }

    private static (int x, int y)? Project(double lon, double lat, int z, int tileX, int tileY)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(tileX, tileY, z);
        if (lon < southWest.Lng || lon > northEast.Lng || lat < southWest.Lat || lat > northEast.Lat)
            return null;

        var xRatio = (lon - southWest.Lng) / (northEast.Lng - southWest.Lng);
        var yRatio = (northEast.Lat - lat) / (northEast.Lat - southWest.Lat);
        var localX = (int)Math.Round(xRatio * Extent);
        var localY = (int)Math.Round(yRatio * Extent);
        return (Math.Clamp(localX, 0, Extent), Math.Clamp(localY, 0, Extent));
    }

    private static ulong ZigZag(int value)
        => (ulong)((value << 1) ^ (value >> 31));

    private static ulong CommandInteger(int id, int count)
        => (ulong)((count << 3) | id);
}
