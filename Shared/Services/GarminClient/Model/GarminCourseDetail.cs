using System.Text.Json.Serialization;

namespace Shared.Services.GarminClient.Model;

public record GarminCourseDetail(
    [property: JsonPropertyName("courseId")] long CourseId,
    [property: JsonPropertyName("courseName")] string CourseName,
    [property: JsonPropertyName("description")] string? Description,
    [property: JsonPropertyName("openStreetMap")] bool OpenStreetMap,
    [property: JsonPropertyName("matchedToSegments")] bool MatchedToSegments,
    [property: JsonPropertyName("userProfilePk")] long UserProfilePk,
    [property: JsonPropertyName("userGroupPk")] long? UserGroupPk,
    [property: JsonPropertyName("rulePK")] int RulePk,
    [property: JsonPropertyName("firstName")] string? FirstName,
    [property: JsonPropertyName("lastName")] string? LastName,
    [property: JsonPropertyName("displayName")] string? DisplayName,
    [property: JsonPropertyName("geoRoutePk")] long GeoRoutePk,
    [property: JsonPropertyName("sourceTypeId")] int SourceTypeId,
    [property: JsonPropertyName("sourcePk")] long? SourcePk,
    [property: JsonPropertyName("distanceMeter")] double DistanceMeter,
    [property: JsonPropertyName("elevationGainMeter")] double ElevationGainMeter,
    [property: JsonPropertyName("elevationLossMeter")] double ElevationLossMeter,
    [property: JsonPropertyName("startPoint")] GarminGeoPoint? StartPoint,
    [property: JsonPropertyName("coursePoints")] IReadOnlyList<GarminCoursePoint>? CoursePoints,
    [property: JsonPropertyName("boundingBox")] GarminBoundingBox? BoundingBox,
    [property: JsonPropertyName("hasShareableEvent")] bool HasShareableEvent,
    [property: JsonPropertyName("hasTurnDetectionDisabled")] bool HasTurnDetectionDisabled,
    [property: JsonPropertyName("activityTypePk")] int ActivityTypePk,
    [property: JsonPropertyName("virtualPartnerId")] long? VirtualPartnerId,
    [property: JsonPropertyName("includeLaps")] bool IncludeLaps,
    [property: JsonPropertyName("elapsedSeconds")] int? ElapsedSeconds,
    [property: JsonPropertyName("speedMeterPerSecond")] double? SpeedMeterPerSecond,
    [property: JsonPropertyName("createDate")] DateTime? CreateDate,
    [property: JsonPropertyName("updateDate")] DateTime? UpdateDate,
    [property: JsonPropertyName("courseLines")] IReadOnlyList<GarminCourseLine>? CourseLines,
    [property: JsonPropertyName("coordinateSystem")] string? CoordinateSystem,
    [property: JsonPropertyName("targetCoordinateSystem")] string? TargetCoordinateSystem,
    [property: JsonPropertyName("originalCoordinateSystem")] string? OriginalCoordinateSystem,
    [property: JsonPropertyName("consumer")] object? Consumer,
    [property: JsonPropertyName("elevationSource")] int? ElevationSource,
    [property: JsonPropertyName("hasPaceBand")] bool HasPaceBand,
    [property: JsonPropertyName("hasPowerGuide")] bool HasPowerGuide,
    [property: JsonPropertyName("favorite")] bool Favorite,
    [property: JsonPropertyName("startNote")] string? StartNote,
    [property: JsonPropertyName("finishNote")] string? FinishNote,
    [property: JsonPropertyName("cutoffDuration")] int? CutoffDuration,
    [property: JsonPropertyName("geoPoints")] IReadOnlyList<GarminGeoPoint>? GeoPoints
);

public record GarminGeoPoint(
    [property: JsonPropertyName("latitude")] double Latitude,
    [property: JsonPropertyName("longitude")] double Longitude,
    [property: JsonPropertyName("elevation")] double? Elevation,
    [property: JsonPropertyName("distance")] double? Distance,
    [property: JsonPropertyName("timestamp")] long? Timestamp
);

public record GarminCoursePoint(
    [property: JsonPropertyName("coursePointId")] long CoursePointId,
    [property: JsonPropertyName("name")] string? Name,
    [property: JsonPropertyName("coursePk")] long CoursePk,
    [property: JsonPropertyName("coursePointType")] string? CoursePointType,
    [property: JsonPropertyName("lon")] double Longitude,
    [property: JsonPropertyName("lat")] double Latitude,
    [property: JsonPropertyName("distance")] double? Distance,
    [property: JsonPropertyName("elevation")] double? Elevation,
    [property: JsonPropertyName("derivedElevation")] double? DerivedElevation,
    [property: JsonPropertyName("timestamp")] long? Timestamp,
    [property: JsonPropertyName("createdDate")] DateTime? CreatedDate,
    [property: JsonPropertyName("modifiedDate")] DateTime? ModifiedDate,
    [property: JsonPropertyName("uuid")] string? Uuid,
    [property: JsonPropertyName("note")] string? Note,
    [property: JsonPropertyName("cutoffDuration")] int? CutoffDuration,
    [property: JsonPropertyName("restDuration")] int? RestDuration
);

public record GarminBoundingBox(
    [property: JsonPropertyName("center")] GarminLatLon? Center,
    [property: JsonPropertyName("lowerLeft")] GarminLatLon? LowerLeft,
    [property: JsonPropertyName("upperRight")] GarminLatLon? UpperRight,
    [property: JsonPropertyName("lowerLeftLatIsSet")] bool LowerLeftLatIsSet,
    [property: JsonPropertyName("lowerLeftLongIsSet")] bool LowerLeftLongIsSet,
    [property: JsonPropertyName("upperRightLatIsSet")] bool UpperRightLatIsSet,
    [property: JsonPropertyName("upperRightLongIsSet")] bool UpperRightLongIsSet
);

public record GarminLatLon(
    [property: JsonPropertyName("latitude")] double Latitude,
    [property: JsonPropertyName("longitude")] double Longitude
);

public record GarminCourseLine(
    [property: JsonPropertyName("courseId")] long CourseId,
    [property: JsonPropertyName("sortOrder")] int SortOrder,
    [property: JsonPropertyName("numberOfPoints")] int NumberOfPoints,
    [property: JsonPropertyName("distanceInMeters")] double DistanceInMeters,
    [property: JsonPropertyName("bearing")] double? Bearing,
    [property: JsonPropertyName("points")] IReadOnlyList<GarminGeoPoint>? Points,
    [property: JsonPropertyName("coordinateSystem")] string? CoordinateSystem,
    [property: JsonPropertyName("originalCoordinateSystem")] string? OriginalCoordinateSystem
);