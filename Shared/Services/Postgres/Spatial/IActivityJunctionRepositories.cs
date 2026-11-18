namespace Shared.Services.Postgres.Spatial;

/// <summary>One row of <c>activity_summits</c>.</summary>
public sealed record SummitLink(string ActivityId, string UserId, string PeakId, DateTime CreatedAt);

/// <summary>One row of <c>activity_visited_areas</c>.</summary>
public sealed record VisitedAreaLink(string ActivityId, string UserId, string AreaId, DateTime CreatedAt);

/// <summary>One row of <c>activity_visited_paths</c>.</summary>
public sealed record VisitedPathLink(
    string ActivityId,
    string UserId,
    string OsmHighwayId,
    string? Name,
    string? Type,
    int? TileX,
    int? TileY,
    DateTime CreatedAt);

/// <summary>Input for inserting a visited path; extra columns are optional.</summary>
public sealed record VisitedPathInput(
    string ActivityId,
    string UserId,
    string OsmHighwayId,
    string? Name = null,
    string? Type = null,
    int? TileX = null,
    int? TileY = null);

/// <summary>Idempotent writes and lookups for <c>activity_summits</c>.</summary>
public interface IActivitySummitRepository
{
    Task Add(string activityId, string userId, string peakId, CancellationToken cancellationToken = default);
    Task AddRange(IEnumerable<SummitLink> links, CancellationToken cancellationToken = default);
    Task Remove(string activityId, string peakId, CancellationToken cancellationToken = default);
    Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<SummitLink>> GetByUser(string userId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<SummitLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByUser(string userId, CancellationToken cancellationToken = default);
}

/// <summary>Idempotent writes and lookups for <c>activity_visited_areas</c>.</summary>
public interface IActivityVisitedAreaRepository
{
    Task Add(string activityId, string userId, string areaId, CancellationToken cancellationToken = default);
    Task AddRange(IEnumerable<VisitedAreaLink> links, CancellationToken cancellationToken = default);
    Task Remove(string activityId, string areaId, CancellationToken cancellationToken = default);
    Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<VisitedAreaLink>> GetByUser(string userId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<VisitedAreaLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByUser(string userId, CancellationToken cancellationToken = default);
}

/// <summary>Idempotent writes and lookups for <c>activity_visited_paths</c>.</summary>
public interface IActivityVisitedPathRepository
{
    /// <summary>Inserts a path reference with only the required columns.</summary>
    Task Add(string activityId, string userId, string osmHighwayId, CancellationToken cancellationToken = default);

    /// <summary>Inserts a path reference including the optional name/type/tile columns.</summary>
    Task AddPath(VisitedPathInput path, CancellationToken cancellationToken = default);

    Task AddRange(IEnumerable<VisitedPathInput> paths, CancellationToken cancellationToken = default);
    Task Remove(string activityId, string osmHighwayId, CancellationToken cancellationToken = default);
    Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<VisitedPathLink>> GetByUser(string userId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<VisitedPathLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByUser(string userId, CancellationToken cancellationToken = default);
}
