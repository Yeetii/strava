using System.Text.Json.Serialization;

namespace Shared.Models;

public class User : IDocument
{
    public required string Id { get; set; }
    public string? UserName { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? RefreshToken { get; set; }
    public string? AccessToken { get; set; }
    public long TokenExpiresAt { get; set; }
    public StravaSyncStatus? SyncStatus { get; set; }

    [JsonPropertyName("_etag")]
    public string? ETag { get; set; }
}

public class StravaSyncStatus
{
    public int? TotalActivitiesOnStrava { get; set; }
    public int SyncedActivities { get; set; }
    public required StravaProcessedActivityCounts ProcessedActivities { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public class StravaProcessedActivityCounts
{
    public int SummitedPeaks { get; set; }
    public int VisitedPaths { get; set; }
    public int VisitedAreas { get; set; }
}