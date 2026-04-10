namespace Shared.Models;

/// <summary>
/// Represents a cached Overpass API result for a specific tile and query.
/// The intended Cosmos DB partition key path is /partitionKey (value: "{x}_{y}").
/// An index on x, y, zoom, and queryHash is recommended.
/// </summary>
public class OverpassCacheDocument : IDocument
{
    /// <summary>Unique document id: "{queryHash}-{zoom}-{x}-{y}"</summary>
    public required string Id { get; set; }

    /// <summary>Cosmos partition key value: "{x}_{y}"</summary>
    public required string PartitionKey { get; set; }

    public required int X { get; set; }
    public required int Y { get; set; }
    public required int Zoom { get; set; }

    /// <summary>SHA-256 hash (hex, lower-case) of the raw overpass query string.</summary>
    public required string QueryHash { get; set; }

    /// <summary>Serialized GeoJSON FeatureCollection JSON string.</summary>
    public required string FeaturesJson { get; set; }

    public static string MakeId(string queryHash, int zoom, int x, int y) =>
        $"{queryHash}-{zoom}-{x}-{y}";

    public static string MakePartitionKey(int x, int y) => $"{x}_{y}";
}
