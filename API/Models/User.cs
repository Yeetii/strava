using System.Text.Json.Serialization;

namespace API.Models;

public class User
    {
        [JsonPropertyName("id")]
        public required string Id { get; set; }
        [JsonPropertyName("userName")]
        public string? UserName { get; set; }
        [JsonPropertyName("refreshToken")]
        public string? RefreshToken { get; set; }
        [JsonPropertyName("accessToken")]
        public string? AccessToken { get; set; }
        [JsonPropertyName("tokenExpiresAt")]
        public long TokenExpiresAt { get; set; }
    }