using System.Text.Json.Serialization;

namespace Shared.Models;

public class VisitedArea : IDocument
{
    [JsonPropertyName("id")]
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public required string AreaId { get; set; }
    public required string Name { get; set; }
    public required string AreaType { get; set; }
    public string? Wikidata { get; set; }
    public string? WikimediaCommons { get; set; }
    public required HashSet<string> ActivityIds { get; set; }
}
