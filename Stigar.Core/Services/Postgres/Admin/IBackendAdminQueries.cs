using BAMCIS.GeoJSON;
using Stigar.Core.Models;

namespace Stigar.Core.Services.Postgres.Admin;

/// <summary>
/// Lightweight activity projection used by the Backend queue/backfill/cutover paths. It carries
/// only the fields those paths need instead of materialising a whole <see cref="Activity"/> row.
/// </summary>
public sealed record BackendActivityProjection(
    string Id,
    string UserId,
    string? SportType,
    ActivityProcessingStatus? ProcessingStatus);

/// <summary>Selects which activity-link junction table a cleanup operation targets.</summary>
public enum BackendLinkFeature
{
    /// <summary>activity_visited_paths</summary>
    Paths,
    /// <summary>activity_summits</summary>
    Peaks,
    /// <summary>activity_visited_areas</summary>
    Areas,
}

/// <summary>
/// Backend-only PostgreSQL queries that do not belong to the shared core or spatial repositories.
/// </summary>
public interface IBackendAdminQueries
{
    /// <summary>Activity id/user/sport/status projections, optionally scoped to one user.</summary>
    Task<IReadOnlyList<BackendActivityProjection>> GetActivityProjections(
        string? userId = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Activity ids whose processing status still has at least one pending stage and has not been
    /// enqueued inside the dedup window. Replaces the activities Cosmos change feed; ordered by id
    /// so polling is deterministic.
    /// </summary>
    Task<IReadOnlyList<string>> GetActivityIdsNeedingProcessing(
        long summitsEpoch,
        long pathsEpoch,
        long areasEpoch,
        int limit,
        CancellationToken cancellationToken = default);

    /// <summary>Marks activities as claimed by the polling job so later polls skip them.</summary>
    Task MarkActivitiesEnqueued(
        IReadOnlyCollection<string> activityIds,
        DateTime queuedAtUtc,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Ids of non-empty admin boundaries whose metrics epoch is behind <paramref name="epoch"/>
    /// and which have not been queued inside the dedup window. Keyset paging via
    /// <paramref name="afterId"/>. Replaces the OSM-features Cosmos change feed.
    /// </summary>
    Task<IReadOnlyList<string>> GetStaleAdminBoundaryIds(
        long epoch,
        string? afterId,
        int limit,
        CancellationToken cancellationToken = default);

    /// <summary>Marks admin boundaries as claimed by the polling job so later polls skip them.</summary>
    Task MarkAdminBoundariesQueued(
        IReadOnlyCollection<string> boundaryIds,
        DateTime queuedAtUtc,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Protected areas whose centroid lies inside the boundary geometry. Preserves the old
    /// enricher's "centroid within boundary" metric semantics using PostGIS.
    /// </summary>
    Task<IReadOnlyList<StoredFeatureSummary>> FindProtectedAreaSummariesWithinBoundary(
        Geometry boundary,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Distinct activity ids that have at least one visited-path row with an empty OSM highway
    /// id. Deterministic keyset paging via <paramref name="afterActivityId"/>.
    /// </summary>
    Task<IReadOnlyList<string>> GetActivityIdsWithMissingOsmHighwayId(
        string? afterActivityId,
        int limit,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes (or, when <paramref name="dryRun"/> is true, only counts) junction rows that link
    /// the supplied activities to the selected feature. Returns the number of affected rows.
    /// </summary>
    Task<int> RemoveExcludedActivityLinks(
        BackendLinkFeature feature,
        IReadOnlyCollection<string> activityIds,
        bool dryRun,
        CancellationToken cancellationToken = default);
}
