namespace Shared.Services.Shards;

internal static class ProtoWire
{
    public static void WriteTag(Stream stream, int fieldNumber, int wireType)
        => WriteVarint(stream, (ulong)((fieldNumber << 3) | wireType));

    public static void WriteVarint(Stream stream, ulong value)
    {
        while (value >= 0x80)
        {
            stream.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        stream.WriteByte((byte)value);
    }

    public static ulong ReadVarint(Stream stream)
    {
        ulong result = 0;
        var shift = 0;
        while (shift < 64)
        {
            int read = stream.ReadByte();
            if (read < 0)
                throw new EndOfStreamException("Unexpected end of stream while reading varint.");

            result |= (ulong)(read & 0x7F) << shift;
            if ((read & 0x80) == 0)
                return result;
            shift += 7;
        }

        throw new InvalidDataException("Invalid varint payload.");
    }

    public static void WriteLengthDelimited(Stream stream, ReadOnlySpan<byte> bytes)
    {
        WriteVarint(stream, (ulong)bytes.Length);
        stream.Write(bytes);
    }

    public static byte[] ReadLengthDelimited(Stream stream)
    {
        var length = (int)ReadVarint(stream);
        var buffer = new byte[length];
        var read = stream.Read(buffer, 0, length);
        if (read != length)
            throw new EndOfStreamException("Unexpected end of stream while reading length-delimited payload.");
        return buffer;
    }

    public static void WriteZigZag32(Stream stream, int value)
        => WriteVarint(stream, (ulong)((value << 1) ^ (value >> 31)));

    public static int ReadZigZag32(Stream stream)
    {
        var value = ReadVarint(stream);
        return (int)((value >> 1) ^ (~(value & 1) + 1));
    }

    public static void SkipField(Stream stream, int wireType)
    {
        switch (wireType)
        {
            case 0:
                _ = ReadVarint(stream);
                return;
            case 2:
                var length = (int)ReadVarint(stream);
                stream.Seek(length, SeekOrigin.Current);
                return;
            default:
                throw new InvalidDataException($"Unsupported wire type {wireType}.");
        }
    }
}
