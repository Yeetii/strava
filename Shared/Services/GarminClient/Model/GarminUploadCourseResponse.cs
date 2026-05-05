using System.Text.Json.Serialization;

namespace Shared.Services.GarminClient.Model;

public record GarminUploadCourseResponse(
    [property: JsonPropertyName("status")] string Status,
    [property: JsonPropertyName("result")] GarminUploadCourseResult? Result
);

public record GarminUploadCourseResult(
    [property: JsonPropertyName("detailedImportResult")] GarminDetailedImportResult? DetailedImportResult
);

public record GarminDetailedImportResult(
    [property: JsonPropertyName("uploadId")] long UploadId,
    [property: JsonPropertyName("uploadUuid")] GarminUploadUuid? UploadUuid,
    [property: JsonPropertyName("owner")] long Owner,
    [property: JsonPropertyName("fileSize")] long FileSize,
    [property: JsonPropertyName("processingTime")] int ProcessingTime,
    [property: JsonPropertyName("creationDate")] string? CreationDate,
    [property: JsonPropertyName("ipAddress")] string? IpAddress,
    [property: JsonPropertyName("fileName")] string? FileName,
    [property: JsonPropertyName("report")] object? Report,
    [property: JsonPropertyName("successes")] IReadOnlyList<object>? Successes,
    [property: JsonPropertyName("failures")] IReadOnlyList<object>? Failures
);

public record GarminUploadUuid(
    [property: JsonPropertyName("uuid")] string? Uuid
);