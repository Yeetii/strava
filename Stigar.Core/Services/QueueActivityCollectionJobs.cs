using System.Security.Cryptography;
using System.Text;
using Hangfire;
using Stigar.Core.Jobs;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Core.Services;

/// <summary>
/// Enqueues per-activity enrichment jobs as typed Hangfire calls. Shared by the
/// API's manual backfill endpoints and the Workers host.
/// </summary>
public static class QueueActivityCollectionJobs
{
    public static Task<int> QueueActivityIdsAsync(
        IEnumerable<string> activityIds,
        IBackgroundJobClient jobs,
        string queueName,
        CancellationToken cancellationToken = default)
    {
        var ids = activityIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var id in ids)
            EnqueueForQueue(jobs, queueName, ActivityIdJob.From(id));

        return Task.FromResult(ids.Count);
    }

    public static async Task QueueAllActivitiesAsync(
        IActivityRepository activities,
        IBackgroundJobClient jobs,
        string queueName,
        string? userId = null,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> activityIds = string.IsNullOrWhiteSpace(userId)
            ? await activities.GetAllIds(cancellationToken)
            : (await activities.GetAllByUser(userId, cancellationToken)).Select(a => a.Id).ToList();

        await QueueActivityIdsAsync(activityIds, jobs, queueName, cancellationToken);
    }

    public static async Task QueueActivitiesExcludingTypesAsync(
        IBackendAdminQueries queries,
        IActivityRepository activities,
        IBackgroundJobClient jobs,
        string queueName,
        IReadOnlySet<string> excludedSportTypes,
        ActivitySyncStage stage,
        string? userId = null,
        CancellationToken cancellationToken = default)
    {
        var projections = await queries.GetActivityProjections(userId, cancellationToken);

        var included = new List<string>();
        var excluded = new List<BackendActivityProjection>();
        foreach (var p in projections)
        {
            if (string.IsNullOrEmpty(p.SportType) || !excludedSportTypes.Contains(p.SportType))
                included.Add(p.Id);
            else
                excluded.Add(p);
        }

        await QueueActivityIdsAsync(included, jobs, queueName, cancellationToken);
        await MarkExcludedActivitiesAsync(activities, excluded, stage, cancellationToken);
    }

    private static void EnqueueForQueue(IBackgroundJobClient jobs, string queueName, ActivityIdJob job)
    {
        switch (queueName)
        {
            case Constants.QueueConfig.CalculateSummitsJobs:
                jobs.EnqueueSummits(job);
                break;
            case Constants.QueueConfig.CalculateVisitedPathsJobs:
                jobs.EnqueueVisitedPaths(job);
                break;
            case Constants.QueueConfig.CalculateVisitedAreasJobs:
                jobs.EnqueueVisitedAreas(job);
                break;
            default:
                throw new InvalidOperationException($"No typed job registered for queue '{queueName}'");
        }
    }

    internal static string BuildQueueAllMessageId(string queueName, string activityId)
    {
        var normalizedQueueName = string.IsNullOrWhiteSpace(queueName)
            ? "unknown-queue"
            : queueName.Trim().ToLowerInvariant();
        var normalizedActivityId = string.IsNullOrWhiteSpace(activityId)
            ? "empty"
            : activityId.Trim();
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes($"queue-all|{normalizedQueueName}|{normalizedActivityId}"));
        var hash = Convert.ToHexString(bytes[..12]).ToLowerInvariant();
        return $"queue-all:{normalizedQueueName}:{hash}";
    }

    // A sport-type exclusion is permanent, so mark the stage as done for every future epoch.
    private const long ExcludedEpoch = long.MaxValue;

    private static async Task MarkExcludedActivitiesAsync(
        IActivityRepository activities,
        List<BackendActivityProjection> excluded,
        ActivitySyncStage stage,
        CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        foreach (var p in excluded)
        {
            var current = p.ProcessingStatus;
            if (GetStageEpoch(current, stage) == ExcludedEpoch && current?.SportTypeExcluded == true)
                continue;

            var next = new ActivityProcessingStatus
            {
                SummitedPeaksEpoch = current?.SummitedPeaksEpoch ?? 0,
                VisitedPathsEpoch = current?.VisitedPathsEpoch ?? 0,
                VisitedAreasEpoch = current?.VisitedAreasEpoch ?? 0,
                SummitedPeaksDoneAtUtc = current?.SummitedPeaksDoneAtUtc,
                VisitedPathsDoneAtUtc = current?.VisitedPathsDoneAtUtc,
                VisitedAreasDoneAtUtc = current?.VisitedAreasDoneAtUtc,
                LastUpdatedAtUtc = now,
                LastProcessingError = current?.LastProcessingError,
                SportTypeExcluded = true,
                EnqueuedAtUtc = current?.EnqueuedAtUtc,
            };
            switch (stage)
            {
                case ActivitySyncStage.SummitedPeaks:
                    next.SummitedPeaksEpoch = ExcludedEpoch;
                    next.SummitedPeaksDoneAtUtc = now;
                    break;
                case ActivitySyncStage.VisitedPaths:
                    next.VisitedPathsEpoch = ExcludedEpoch;
                    next.VisitedPathsDoneAtUtc = now;
                    break;
                case ActivitySyncStage.VisitedAreas:
                    next.VisitedAreasEpoch = ExcludedEpoch;
                    next.VisitedAreasDoneAtUtc = now;
                    break;
            }
            await activities.UpdateProcessingStatus(p.Id, next, cancellationToken);
        }
    }

    private static long GetStageEpoch(ActivityProcessingStatus? status, ActivitySyncStage stage)
        => stage switch
        {
            ActivitySyncStage.SummitedPeaks => status?.SummitedPeaksEpoch ?? 0,
            ActivitySyncStage.VisitedPaths => status?.VisitedPathsEpoch ?? 0,
            ActivitySyncStage.VisitedAreas => status?.VisitedAreasEpoch ?? 0,
            _ => 0,
        };
}
