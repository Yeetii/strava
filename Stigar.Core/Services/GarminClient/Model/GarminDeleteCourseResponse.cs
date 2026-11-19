using System.Text.Json.Serialization;

namespace Stigar.Core.Services.GarminClient.Model;

public record GarminDeleteCourseResponse(
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("course_id")] long CourseId
);