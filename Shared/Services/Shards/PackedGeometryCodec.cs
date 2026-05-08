using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Services.Shards;

public static class PackedGeometryCodec
{
    private const double Scale = 1e7;

    public static (ShardFeatureType type, byte[] bytes) Encode(Geometry geometry)
    {
        return geometry switch
        {
            Point point => (ShardFeatureType.Point, EncodeCoordinates([ToCoordinate(point.Coordinates)])),
            LineString line => (ShardFeatureType.LineString, EncodeCoordinates(line.Coordinates.Select(ToCoordinate))),
            _ => throw new NotSupportedException($"Geometry type {geometry.Type} is not supported in shard codec.")
        };
    }

    public static Geometry Decode(ShardFeatureType type, ReadOnlySpan<byte> bytes)
    {
        var coordinates = DecodeCoordinates(bytes);
        return type switch
        {
            ShardFeatureType.Point when coordinates.Count > 0 => new Point(new Position(coordinates[0].Lng, coordinates[0].Lat)),
            ShardFeatureType.LineString when coordinates.Count > 1 => new LineString(coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList()),
            _ => throw new NotSupportedException($"Shard feature type {type} is not supported for decoding.")
        };
    }

    private static byte[] EncodeCoordinates(IEnumerable<Coordinate> coordinates)
    {
        using var stream = new MemoryStream();
        int prevLon = 0;
        int prevLat = 0;
        foreach (var coordinate in coordinates)
        {
            var lon = (int)Math.Round(coordinate.Lng * Scale);
            var lat = (int)Math.Round(coordinate.Lat * Scale);
            ProtoWire.WriteZigZag32(stream, lon - prevLon);
            ProtoWire.WriteZigZag32(stream, lat - prevLat);
            prevLon = lon;
            prevLat = lat;
        }

        return stream.ToArray();
    }

    private static List<Coordinate> DecodeCoordinates(ReadOnlySpan<byte> bytes)
    {
        var result = new List<Coordinate>();
        using var stream = new MemoryStream(bytes.ToArray());
        int lon = 0;
        int lat = 0;
        while (stream.Position < stream.Length)
        {
            lon += ProtoWire.ReadZigZag32(stream);
            lat += ProtoWire.ReadZigZag32(stream);
            result.Add(new Coordinate(lon / Scale, lat / Scale));
        }

        return result;
    }

    private static Coordinate ToCoordinate(Position position) => new(position.Longitude, position.Latitude);
}
