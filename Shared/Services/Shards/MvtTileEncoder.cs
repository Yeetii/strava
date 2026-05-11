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
        var keys = new List<string>();
        var keyLookup = new Dictionary<string, uint>(StringComparer.Ordinal);
        var values = new List<MvtValue>();
        var valueLookup = new Dictionary<MvtValue, uint>();
        var encodedFeatures = new List<byte[]>();

        foreach (var feature in features)
        {
            var encoded = EncodeFeature(feature, z, x, y, keys, keyLookup, values, valueLookup);
            if (encoded.Length > 0)
                encodedFeatures.Add(encoded);
        }

        using var layerStream = new MemoryStream();
        ProtoWire.WriteTag(layerStream, 15, 0); // version
        ProtoWire.WriteVarint(layerStream, 2);
        ProtoWire.WriteTag(layerStream, 1, 2); // name
        ProtoWire.WriteLengthDelimited(layerStream, System.Text.Encoding.UTF8.GetBytes(layerName));

        foreach (var key in keys)
        {
            ProtoWire.WriteTag(layerStream, 3, 2);
            ProtoWire.WriteLengthDelimited(layerStream, System.Text.Encoding.UTF8.GetBytes(key));
        }

        foreach (var value in values)
        {
            ProtoWire.WriteTag(layerStream, 4, 2);
            ProtoWire.WriteLengthDelimited(layerStream, EncodeValue(value));
        }

        foreach (var encoded in encodedFeatures)
        {
            ProtoWire.WriteTag(layerStream, 2, 2);
            ProtoWire.WriteLengthDelimited(layerStream, encoded);
        }

        ProtoWire.WriteTag(layerStream, 5, 0); // extent
        ProtoWire.WriteVarint(layerStream, Extent);

        return layerStream.ToArray();
    }

    private static byte[] EncodeFeature(
        Feature feature,
        int z,
        int x,
        int y,
        List<string> keys,
        Dictionary<string, uint> keyLookup,
        List<MvtValue> values,
        Dictionary<MvtValue, uint> valueLookup)
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

        var tags = EncodeTags(feature.Properties, keys, keyLookup, values, valueLookup);
        if (tags.Length > 0)
        {
            ProtoWire.WriteTag(stream, 2, 2);
            ProtoWire.WriteLengthDelimited(stream, tags);
        }

        ProtoWire.WriteTag(stream, 3, 0);
        ProtoWire.WriteVarint(stream, geometryType);
        ProtoWire.WriteTag(stream, 4, 2);
        ProtoWire.WriteLengthDelimited(stream, commands);
        return stream.ToArray();
    }

    private static byte[] EncodeTags(
        IDictionary<string, dynamic>? properties,
        List<string> keys,
        Dictionary<string, uint> keyLookup,
        List<MvtValue> values,
        Dictionary<MvtValue, uint> valueLookup)
    {
        if (properties == null || properties.Count == 0)
            return [];

        using var stream = new MemoryStream();
        foreach (var property in properties)
        {
            if (string.IsNullOrWhiteSpace(property.Key))
                continue;

            if (!TryCreateValue(property.Value, out MvtValue value))
                continue;

            if (!keyLookup.TryGetValue(property.Key, out var keyIndex))
            {
                keyIndex = (uint)keys.Count;
                keys.Add(property.Key);
                keyLookup[property.Key] = keyIndex;
            }

            if (!valueLookup.TryGetValue(value, out var valueIndex))
            {
                valueIndex = (uint)values.Count;
                values.Add(value);
                valueLookup[value] = valueIndex;
            }

            ProtoWire.WriteVarint(stream, keyIndex);
            ProtoWire.WriteVarint(stream, valueIndex);
        }

        return stream.ToArray();
    }

    private static bool TryCreateValue(dynamic? rawValue, out MvtValue value)
    {
        value = default;

        if (rawValue is null)
            return false;

        if (rawValue is string stringValue)
        {
            value = new MvtValue(MvtValueKind.String, stringValue);
            return true;
        }

        if (rawValue is bool boolValue)
        {
            value = new MvtValue(MvtValueKind.Bool, boolValue ? "true" : "false");
            return true;
        }

        if (rawValue is sbyte or byte or short or ushort or int or uint or long or ulong)
        {
            value = new MvtValue(MvtValueKind.Int64, Convert.ToString(rawValue, System.Globalization.CultureInfo.InvariantCulture)!);
            return true;
        }

        if (rawValue is float or double or decimal)
        {
            value = new MvtValue(MvtValueKind.Double, Convert.ToString(rawValue, System.Globalization.CultureInfo.InvariantCulture)!);
            return true;
        }

        value = new MvtValue(MvtValueKind.String, Convert.ToString(rawValue, System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty);
        return true;
    }

    private static byte[] EncodeValue(MvtValue value)
    {
        using var stream = new MemoryStream();
        switch (value.Kind)
        {
            case MvtValueKind.String:
                ProtoWire.WriteTag(stream, 1, 2);
                ProtoWire.WriteLengthDelimited(stream, System.Text.Encoding.UTF8.GetBytes(value.Value));
                break;
            case MvtValueKind.Double:
                ProtoWire.WriteTag(stream, 3, 1);
                var doubleBytes = BitConverter.GetBytes(double.Parse(value.Value, System.Globalization.CultureInfo.InvariantCulture));
                stream.Write(doubleBytes, 0, doubleBytes.Length);
                break;
            case MvtValueKind.Int64:
                ProtoWire.WriteTag(stream, 4, 0);
                ProtoWire.WriteVarint(stream, unchecked((ulong)long.Parse(value.Value, System.Globalization.CultureInfo.InvariantCulture)));
                break;
            case MvtValueKind.Bool:
                ProtoWire.WriteTag(stream, 7, 0);
                ProtoWire.WriteVarint(stream, bool.Parse(value.Value) ? 1u : 0u);
                break;
        }

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

    private enum MvtValueKind
    {
        String,
        Double,
        Int64,
        Bool
    }

    private readonly record struct MvtValue(MvtValueKind Kind, string Value);
}
