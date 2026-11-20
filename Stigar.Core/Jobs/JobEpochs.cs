namespace Stigar.Core.Jobs;

/// <summary>Job types whose rows can be marked for rediscovery by bumping an epoch.</summary>
public enum JobEpochType
{
    ActivitySummits,
    ActivityVisitedPaths,
    ActivityVisitedAreas,
    AdminBoundaryMetrics,
}

/// <summary>
/// Stable registry keys. These strings are persisted in job_epochs.job_type and must not be
/// renamed without a migration, since doing so would strand existing epoch values.
/// </summary>
public static class JobEpochKeys
{
    public const string ActivitySummits = "activity.summits";
    public const string ActivityVisitedPaths = "activity.visitedPaths";
    public const string ActivityVisitedAreas = "activity.visitedAreas";
    public const string AdminBoundaryMetrics = "adminBoundary.metrics";

    public static string For(JobEpochType type) => type switch
    {
        JobEpochType.ActivitySummits => ActivitySummits,
        JobEpochType.ActivityVisitedPaths => ActivityVisitedPaths,
        JobEpochType.ActivityVisitedAreas => ActivityVisitedAreas,
        JobEpochType.AdminBoundaryMetrics => AdminBoundaryMetrics,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
    };

    public static bool TryParse(string? key, out JobEpochType type)
    {
        type = key switch
        {
            ActivitySummits => JobEpochType.ActivitySummits,
            ActivityVisitedPaths => JobEpochType.ActivityVisitedPaths,
            ActivityVisitedAreas => JobEpochType.ActivityVisitedAreas,
            AdminBoundaryMetrics => JobEpochType.AdminBoundaryMetrics,
            _ => default,
        };
        return key is not null && For(type) == key;
    }

    public static IEnumerable<string> All =>
    [
        ActivitySummits,
        ActivityVisitedPaths,
        ActivityVisitedAreas,
        AdminBoundaryMetrics,
    ];
}

public interface IJobEpochService
{
    Task<long> GetAsync(JobEpochType type, CancellationToken cancellationToken = default);
    Task<long> BumpAsync(JobEpochType type, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<(string JobType, long Epoch)>> GetAllAsync(CancellationToken cancellationToken = default);
}
