using System.Text.Json.Serialization;

namespace Shared.Services.StravaClient.Model;

public record StravaRoute(
    [property: JsonPropertyName("id")] long Id,
    [property: JsonPropertyName("id_str")] string IdStr,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("description")] string? Description,
    [property: JsonPropertyName("distance")] float? Distance,
    [property: JsonPropertyName("elevation_gain")] float? ElevationGain,
    [property: JsonPropertyName("type")] int? Type,
    [property: JsonPropertyName("sub_type")] int? SubType,
    [property: JsonPropertyName("private")] bool? Private,
    [property: JsonPropertyName("starred")] bool? Starred,
    [property: JsonPropertyName("timestamp")] long? Timestamp,
    [property: JsonPropertyName("created_at")] DateTime? CreatedAt,
    [property: JsonPropertyName("updated_at")] DateTime? UpdatedAt,
    [property: JsonPropertyName("map")] Map? Map,
    [property: JsonPropertyName("athlete")] Metadata? Athlete
);

public record UploadStatus(
    [property: JsonPropertyName("id")] long Id,
    [property: JsonPropertyName("id_str")] string IdStr,
    [property: JsonPropertyName("external_id")] string? ExternalId,
    [property: JsonPropertyName("error")] string? Error,
    [property: JsonPropertyName("status")] string? Status,
    [property: JsonPropertyName("activity_id")] long? ActivityId
);
