using Shared.Models;

namespace Shared.Services.Shards;

public static class ShardBinarySerializer
{
    public static byte[] Serialize(Shard shard)
    {
        using var stream = new MemoryStream();
        foreach (var feature in shard.Owned)
        {
            ProtoWire.WriteTag(stream, 1, 2);
            var bytes = SerializeFeature(feature);
            ProtoWire.WriteLengthDelimited(stream, bytes);
        }

        return stream.ToArray();
    }

    public static Shard Deserialize(ReadOnlySpan<byte> payload)
    {
        var shard = new Shard();
        using var stream = new MemoryStream(payload.ToArray());
        while (stream.Position < stream.Length)
        {
            var tag = (int)ProtoWire.ReadVarint(stream);
            var fieldNumber = tag >> 3;
            var wireType = tag & 0x7;

            switch (fieldNumber)
            {
                case 1 when wireType == 2:
                    shard.Owned.Add(DeserializeFeature(ProtoWire.ReadLengthDelimited(stream)));
                    break;
                default:
                    ProtoWire.SkipField(stream, wireType);
                    break;
            }
        }

        return shard;
    }

    private static byte[] SerializeFeature(ShardFeature feature)
    {
        using var stream = new MemoryStream();
        ProtoWire.WriteTag(stream, 1, 0);
        ProtoWire.WriteVarint(stream, feature.Id);

        if (!string.IsNullOrWhiteSpace(feature.OsmId))
        {
            ProtoWire.WriteTag(stream, 5, 2);
            ProtoWire.WriteLengthDelimited(stream, System.Text.Encoding.UTF8.GetBytes(feature.OsmId));
        }

        if (!string.IsNullOrWhiteSpace(feature.Name))
        {
            ProtoWire.WriteTag(stream, 6, 2);
            ProtoWire.WriteLengthDelimited(stream, System.Text.Encoding.UTF8.GetBytes(feature.Name));
        }

        ProtoWire.WriteTag(stream, 2, 0);
        ProtoWire.WriteVarint(stream, (uint)feature.Type);

        ProtoWire.WriteTag(stream, 3, 2);
        ProtoWire.WriteLengthDelimited(stream, feature.Geometry);

        foreach (var tag in feature.Tags)
        {
            ProtoWire.WriteTag(stream, 4, 2);
            ProtoWire.WriteLengthDelimited(stream, SerializeTag(tag));
        }

        return stream.ToArray();
    }

    private static ShardFeature DeserializeFeature(ReadOnlySpan<byte> bytes)
    {
        var feature = new ShardFeature();
        using var stream = new MemoryStream(bytes.ToArray());
        while (stream.Position < stream.Length)
        {
            var tag = (int)ProtoWire.ReadVarint(stream);
            var fieldNumber = tag >> 3;
            var wireType = tag & 0x7;
            switch (fieldNumber)
            {
                case 1 when wireType == 0:
                    feature.Id = ProtoWire.ReadVarint(stream);
                    break;
                case 5 when wireType == 2:
                    feature.OsmId = System.Text.Encoding.UTF8.GetString(ProtoWire.ReadLengthDelimited(stream));
                    break;
                case 6 when wireType == 2:
                    feature.Name = System.Text.Encoding.UTF8.GetString(ProtoWire.ReadLengthDelimited(stream));
                    break;
                case 2 when wireType == 0:
                    feature.Type = (ShardFeatureType)ProtoWire.ReadVarint(stream);
                    break;
                case 3 when wireType == 2:
                    feature.Geometry = ProtoWire.ReadLengthDelimited(stream);
                    break;
                case 4 when wireType == 2:
                    feature.Tags.Add(DeserializeTag(ProtoWire.ReadLengthDelimited(stream)));
                    break;
                default:
                    ProtoWire.SkipField(stream, wireType);
                    break;
            }
        }

        return feature;
    }

    private static byte[] SerializeTag(ShardTag tag)
    {
        using var stream = new MemoryStream();
        ProtoWire.WriteTag(stream, 1, 0);
        ProtoWire.WriteVarint(stream, tag.KeyId);
        ProtoWire.WriteTag(stream, 2, 0);
        ProtoWire.WriteVarint(stream, tag.ValueId);
        return stream.ToArray();
    }

    private static ShardTag DeserializeTag(ReadOnlySpan<byte> bytes)
    {
        var tag = new ShardTag();
        using var stream = new MemoryStream(bytes.ToArray());
        while (stream.Position < stream.Length)
        {
            var header = (int)ProtoWire.ReadVarint(stream);
            var fieldNumber = header >> 3;
            var wireType = header & 0x7;
            switch (fieldNumber)
            {
                case 1 when wireType == 0:
                    tag.KeyId = (uint)ProtoWire.ReadVarint(stream);
                    break;
                case 2 when wireType == 0:
                    tag.ValueId = (uint)ProtoWire.ReadVarint(stream);
                    break;
                default:
                    ProtoWire.SkipField(stream, wireType);
                    break;
            }
        }

        return tag;
    }
}
