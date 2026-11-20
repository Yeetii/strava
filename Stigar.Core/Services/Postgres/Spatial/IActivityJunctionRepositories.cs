namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>One row of <c>activity_summits</c>.</summary>
public sealed record SummitLink(string ActivityId, string UserId, string PeakId, DateTime CreatedAt);

/// <summary>One row of <c>activity_visited_areas</c>.</summary>
public sealed record VisitedAreaLink(string ActivityId, string UserId, string AreaId, DateTime CreatedAt);

/// <summary>One row of <c>activity_visited_tiles</c>.</summary>
public sealed record VisitedTileLink(
    string ActivityId,
    string UserId,
    int TileX,
    int TileY,
    long[] HighwayIds);

/// <summary>One visited path; grouped by tile on write.</summary>
public sealed record VisitedPathInput(
    string ActivityId,
    string UserId,
    long OsmHighwayId,
    int TileX,
    int TileY);

/// <summary>Aggregated result returned by the visited-paths endpoint.</summary>
public sealed record VisitedPathSummary(
    long OsmHighwayId,
    int TimesVisited,
    DateTime? FirstDate,
    DateTime? LastDate);

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

/// <summary>Idempotent writes and lookups for <c>activity_visited_tiles</c>.</summary>
public interface IActivityVisitedPathRepository
{
    Task Add(string activityId, string userId, long osmHighwayId, int tileX, int tileY, CancellationToken cancellationToken = default);
    Task AddPath(VisitedPathInput path, CancellationToken cancellationToken = default);
    Task AddRange(IEnumerable<VisitedPathInput> paths, CancellationToken cancellationToken = default);
    Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<VisitedTileLink>> GetByUser(string userId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<VisitedTileLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default);
    Task<int> CountByUser(string userId, CancellationToken cancellationToken = default);

    /// <summary>Aggregates highways across the requested tiles for one user.</summary>
    Task<IReadOnlyList<VisitedPathSummary>> GetSummaryByUserAndTiles(
        string userId, IReadOnlyList<(int x, int y)> tiles, CancellationToken cancellationToken = default);
}
