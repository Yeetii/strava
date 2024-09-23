using System.Text.Json.Serialization;
using Shared.Services;

namespace Shared.Models;

public class SummitedPeak : IDocument
{
    [JsonPropertyName("id")]
    public required string Id { get; set; }
    public required string Name { get; set; }
    public required string UserId { get; set; }
    public required string PeakId { get; set; }
    public float? Elevation { get; set; }
    public required HashSet<string> ActivityIds { get; set; }
}