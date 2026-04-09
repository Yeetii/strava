using System.Text.Json.Serialization;

namespace Shared.Models;

public class VisitedPath : IDocument
{
    [JsonPropertyName("id")]
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public required string PathId { get; set; }
    public required HashSet<string> ActivityIds { get; set; }
}
