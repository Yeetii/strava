using System.Text.Json.Serialization;

namespace Shared.Models;
public class ActivityFetchJob
{
    [JsonPropertyName("userId")]
    public required string UserId { get; set; }
    [JsonPropertyName("page")]
    public int? Page { get; set;}
    [JsonPropertyName("before")]
    public DateTime? Before { get; set; }
    [JsonPropertyName("after")]
    public DateTime? After { get; set; }

}