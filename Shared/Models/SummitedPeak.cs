using System.Text.Json.Serialization;

namespace Shared.Models;

public class SummitedPeak{
    [JsonPropertyName("id")]
    public required string Id { get; set; }
    public required string UserId { get; set; }
    public required string PeakId { get; set; }
    public required HashSet<string> ActivityIds { get; set; }
}