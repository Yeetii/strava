using System.Text.Json.Serialization;

namespace Shared.Jobs;

/// <summary>
/// Strongly-typed payloads for every queue-backed job. Producers (API) and
/// consumers (Workers) both reference this assembly, so a payload is defined
/// exactly once and travels as a typed Hangfire argument rather than an
/// opaque JSON string wrapped in a transport envelope.
/// </summary>
public sealed class AccountDeleteJob
{
    [JsonPropertyName("userId")]
    public required string UserId { get; set; }
}

public sealed class ActivityDeleteJob
{
    [JsonPropertyName("userId")]
    public required string UserId { get; set; }

    [JsonPropertyName("activityId")]
    public required string ActivityId { get; set; }
}

/// <summary>First page (or explicit page range) of an athlete's activity list.</summary>
public sealed class ActivitiesFetchJob
{
    [JsonPropertyName("userId")]
    public required string UserId { get; set; }

    [JsonPropertyName("page")]
    public int? Page { get; set; }

    [JsonPropertyName("before")]
    public DateTime? Before { get; set; }

    [JsonPropertyName("after")]
    public DateTime? After { get; set; }
}

/// <summary>A single Strava activity to fetch and persist.</summary>
public sealed class ActivityFetchJob
{
    [JsonPropertyName("userId")]
    public required string UserId { get; set; }

    [JsonPropertyName("activityId")]
    public required string ActivityId { get; set; }
}

/// <summary>A single activity id for the per-activity enrichment workers.</summary>
public sealed class ActivityIdJob
{
    [JsonPropertyName("activityId")]
    public required string ActivityId { get; set; }

    public static ActivityIdJob From(string activityId) => new() { ActivityId = activityId };
}

/// <summary>A single admin-boundary id for the enrichment worker.</summary>
public sealed class AdminBoundaryIdJob
{
    [JsonPropertyName("boundaryId")]
    public required string BoundaryId { get; set; }

    public static AdminBoundaryIdJob From(string boundaryId) => new() { BoundaryId = boundaryId };
}

/// <summary>One race scrape request, keyed by the canonical organizer key.</summary>
public sealed record ScrapeRaceMessage(string OrganizerKey, bool IsUrgent = false);

/// <summary>One page of a race-discovery agent crawl.</summary>
public sealed record RaceDiscoveryMessage(
    string Agent,
    int? Page = null,
    string? Country = null,
    int? CountryIndex = null,
    int? CountryPage = null,
    int? CountryPageCount = null,
    int? RacesOnPage = null)
{
    [JsonIgnore]
    public int CurrentPage => Page.GetValueOrDefault(1);
}

/// <summary>A manually requested Mistral scrape of a single race URL.</summary>
public sealed record ManualMistralScrapeJob(string Url, string? Name = null);
