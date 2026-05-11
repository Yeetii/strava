namespace Shared.Models;

public enum ShardFeatureType : uint
{
    Unknown = 0,
    Point = 1,
    LineString = 2,
    Polygon = 3
}

public sealed class ShardTag
{
    public uint KeyId { get; set; }
    public uint ValueId { get; set; }
}

public sealed class ShardFeature
{
    public ulong Id { get; set; }
    public string? OsmId { get; set; }
    public string? Name { get; set; }
    public ShardFeatureType Type { get; set; }
    public byte[] Geometry { get; set; } = [];
    public List<ShardTag> Tags { get; set; } = [];
}

public sealed class Shard
{
    public List<ShardFeature> Owned { get; set; } = [];
}
