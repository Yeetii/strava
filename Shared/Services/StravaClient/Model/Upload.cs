using System.Text.Json.Serialization;

namespace Shared.Services.StravaClient.Model;

public record StravaUpload(
    [property: JsonPropertyName("id")] long Id,
    [property: JsonPropertyName("id_str")] string IdStr,
    [property: JsonPropertyName("external_id")] string? ExternalId,
    [property: JsonPropertyName("error")] string? Error,
    [property: JsonPropertyName("status")] string? Status,
    [property: JsonPropertyName("activity_id")] long? ActivityId
);