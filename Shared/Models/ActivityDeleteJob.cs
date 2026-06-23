using System.Text.Json.Serialization;

namespace Shared.Models;

public class ActivityDeleteJob
{
    [JsonPropertyName("userId")]
    public required string UserId { get; set; }

    [JsonPropertyName("activityId")]
    public required string ActivityId { get; set; }
}
