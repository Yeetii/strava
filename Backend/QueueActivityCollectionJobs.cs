using Microsoft.Azure.Functions.Worker;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Admin;
using System.Security.Cryptography;
using System.Text;

namespace Backend;

internal static class QueueActivityCollectionJobs
{
    private static readonly TimeSpan MessageSpacing = TimeSpan.FromSeconds(5);

    public static async Task<int> QueueActivityIdsAsync(
        IEnumerable<string> activityIds,
        QueueClient queueClient,
        string queueName,
        TimeSpan? messageSpacing = null)
    {
        var ids = activityIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (ids.Count == 0)
            return 0;

        var spacing = messageSpacing ?? MessageSpacing;
        var sender = queueClient.CreateSender(queueName);
        for (var index = 0; index < ids.Count; index++)
        {
            await sender.SendMessageAsync(new QueueMessage(ids[index])
            {
                MessageId = BuildQueueAllMessageId(queueName, ids[index]),
                // ScheduledEnqueueTime = DateTimeOffset.UtcNow.Add(spacing * index)
            });
        }

        return ids.Count;
    }

    public static async Task QueueAllActivitiesAsync(
        IActivityRepository activities,
        QueueClient queueClient,
        string queueName,
        string? userId = null,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<string> activityIds = string.IsNullOrWhiteSpace(userId)
            ? await activities.GetAllIds(cancellationToken)
            : (await activities.GetAllByUser(userId, cancellationToken)).Select(a => a.Id).ToList();

        await QueueActivityIdsAsync(activityIds, queueClient, queueName);
    }

    public static async Task QueueActivitiesExcludingTypesAsync(
        IBackendAdminQueries queries,
        IActivityRepository activities,
        QueueClient queueClient,
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

        await QueueActivityIdsAsync(included, queueClient, queueName);
        await MarkExcludedActivitiesAsync(activities, excluded, stage, cancellationToken);
    }

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
            var stageAlreadyDone = stage switch
            {
                ActivitySyncStage.SummitedPeaks => current?.SummitedPeaks == true,
                ActivitySyncStage.VisitedPaths => current?.VisitedPaths == true,
                ActivitySyncStage.VisitedAreas => current?.VisitedAreas == true,
                _ => false,
            };
            if (stageAlreadyDone && current?.SportTypeExcluded == true)
                continue;

            var next = new ActivityProcessingStatus
            {
                SummitedPeaks = current?.SummitedPeaks ?? false,
                VisitedPaths = current?.VisitedPaths ?? false,
                VisitedAreas = current?.VisitedAreas ?? false,
                SummitedPeaksDoneAtUtc = current?.SummitedPeaksDoneAtUtc,
                VisitedPathsDoneAtUtc = current?.VisitedPathsDoneAtUtc,
                VisitedAreasDoneAtUtc = current?.VisitedAreasDoneAtUtc,
                LastUpdatedAtUtc = now,
                LastProcessingError = current?.LastProcessingError,
                SportTypeExcluded = true,
            };
            switch (stage)
            {
                case ActivitySyncStage.SummitedPeaks:
                    next.SummitedPeaks = true;
                    next.SummitedPeaksDoneAtUtc = now;
                    break;
                case ActivitySyncStage.VisitedPaths:
                    next.VisitedPaths = true;
                    next.VisitedPathsDoneAtUtc = now;
                    break;
                case ActivitySyncStage.VisitedAreas:
                    next.VisitedAreas = true;
                    next.VisitedAreasDoneAtUtc = now;
                    break;
            }
            await activities.UpdateProcessingStatus(p.Id, next, cancellationToken);
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
