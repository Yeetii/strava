using System.Text.Json.Serialization;

namespace Shared.Models;

public class VisitedPath : IDocument
{
    [JsonPropertyName("id")]
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public string? OsmHighwayId { get; set; }
    public string? Name { get; set; }
    public string? Type { get; set; }
    public required HashSet<string> ActivityIds { get; set; }
    public int? TileX { get; set; }
    public int? TileY { get; set; }
}
