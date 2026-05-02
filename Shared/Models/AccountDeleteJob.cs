using System.Text.Json.Serialization;

namespace Shared.Models;

public class AccountDeleteJob
{
    [JsonPropertyName("userId")]
    public required string UserId { get; set; }
}
