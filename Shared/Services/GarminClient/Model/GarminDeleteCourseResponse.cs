using System.Text.Json.Serialization;

namespace Shared.Services.GarminClient.Model;

public record GarminDeleteCourseResponse(
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("course_id")] long CourseId
);