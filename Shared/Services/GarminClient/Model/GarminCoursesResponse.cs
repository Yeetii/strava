using System.Text.Json.Serialization;

namespace Shared.Services.GarminClient.Model;

public record GarminCoursesResponse(
    [property: JsonPropertyName("courses")] IReadOnlyList<GarminCourseSummary> Courses
);

public record GarminCourseSummary(
    [property: JsonPropertyName("courseId")] long CourseId,
    [property: JsonPropertyName("userProfileId")] long UserProfileId,
    [property: JsonPropertyName("displayName")] string? DisplayName,
    [property: JsonPropertyName("userGroupId")] long? UserGroupId,
    [property: JsonPropertyName("geoRoutePk")] long? GeoRoutePk,
    [property: JsonPropertyName("activityType")] GarminCourseActivityType? ActivityType,
    [property: JsonPropertyName("courseName")] string CourseName,
    [property: JsonPropertyName("courseDescription")] string? CourseDescription,
    [property: JsonPropertyName("createdDate")] long CreatedDate,
    [property: JsonPropertyName("updatedDate")] long UpdatedDate,
    [property: JsonPropertyName("privacyRule")] GarminPrivacyRule? PrivacyRule,
    [property: JsonPropertyName("distanceInMeters")] double DistanceInMeters,
    [property: JsonPropertyName("elevationGainInMeters")] double ElevationGainInMeters,
    [property: JsonPropertyName("elevationLossInMeters")] double ElevationLossInMeters,
    [property: JsonPropertyName("startLatitude")] double? StartLatitude,
    [property: JsonPropertyName("startLongitude")] double? StartLongitude,
    [property: JsonPropertyName("speedInMetersPerSecond")] double? SpeedInMetersPerSecond,
    [property: JsonPropertyName("sourceTypeId")] int? SourceTypeId,
    [property: JsonPropertyName("sourcePk")] long? SourcePk,
    [property: JsonPropertyName("elapsedSeconds")] int? ElapsedSeconds,
    [property: JsonPropertyName("coordinateSystem")] string? CoordinateSystem,
    [property: JsonPropertyName("originalCoordinateSystem")] string? OriginalCoordinateSystem,
    [property: JsonPropertyName("consumer")] object? Consumer,
    [property: JsonPropertyName("elevationSource")] int? ElevationSource,
    [property: JsonPropertyName("hasShareableEvent")] bool HasShareableEvent,
    [property: JsonPropertyName("hasPaceBand")] bool HasPaceBand,
    [property: JsonPropertyName("hasPowerGuide")] bool HasPowerGuide,
    [property: JsonPropertyName("favorite")] bool Favorite,
    [property: JsonPropertyName("hasTurnDetectionDisabled")] bool HasTurnDetectionDisabled,
    [property: JsonPropertyName("curatedCourseId")] long? CuratedCourseId,
    [property: JsonPropertyName("startNote")] string? StartNote,
    [property: JsonPropertyName("finishNote")] string? FinishNote,
    [property: JsonPropertyName("cutoffDuration")] int? CutoffDuration,
    [property: JsonPropertyName("activityTypeId")] GarminCourseActivityType? ActivityTypeId,
    [property: JsonPropertyName("public")] bool Public,
    [property: JsonPropertyName("createdDateFormatted")] string? CreatedDateFormatted,
    [property: JsonPropertyName("updatedDateFormatted")] string? UpdatedDateFormatted
);

public record GarminCourseActivityType(
    [property: JsonPropertyName("typeId")] int TypeId,
    [property: JsonPropertyName("typeKey")] string? TypeKey,
    [property: JsonPropertyName("parentTypeId")] int? ParentTypeId,
    [property: JsonPropertyName("isHidden")] bool IsHidden,
    [property: JsonPropertyName("restricted")] bool Restricted,
    [property: JsonPropertyName("trimmable")] bool Trimmable
);

public record GarminPrivacyRule(
    [property: JsonPropertyName("typeId")] int TypeId,
    [property: JsonPropertyName("typeKey")] string? TypeKey
);