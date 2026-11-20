using System.Security.Cryptography;
using System.Text;
using Hangfire;
using Stigar.Core.Jobs;
using Stigar.Core.Services.Postgres;

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

}
