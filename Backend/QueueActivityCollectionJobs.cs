using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Services;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace Backend;

internal static class QueueActivityCollectionJobs
{
    private static readonly TimeSpan MessageSpacing = TimeSpan.FromSeconds(5);

    public static async Task<int> QueueActivityIdsAsync(
        IEnumerable<string> activityIds,
        ServiceBusClient serviceBusClient,
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
        var sender = serviceBusClient.CreateSender(queueName);
        for (var index = 0; index < ids.Count; index++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage(ids[index])
            {
                MessageId = BuildQueueAllMessageId(queueName, ids[index]),
                // ScheduledEnqueueTime = DateTimeOffset.UtcNow.Add(spacing * index)
            });
        }

        return ids.Count;
    }

    public static async Task QueueAllActivitiesAsync(
        CollectionClient<Activity> activitiesClient,
        ServiceBusClient serviceBusClient,
        string queueName,
        string? userId = null)
    {
        var activityIds = await activitiesClient.ExecuteQueryAsync<string>(BuildActivityIdsQueryDefinition(userId));
        await QueueActivityIdsAsync(activityIds, serviceBusClient, queueName);
    }

    public static async Task QueueActivitiesExcludingTypesAsync(
        CollectionClient<Activity> activitiesClient,
        ServiceBusClient serviceBusClient,
        string queueName,
        IReadOnlySet<string> excludedSportTypes,
        ActivitySyncStage stage,
        string? userId = null)
    {
        var projections = await activitiesClient.ExecuteQueryAsync<ActivityIdProjection>(
            BuildActivityProjectionsQueryDefinition(userId));

        var included = new List<string>();
        var excluded = new List<ActivityIdProjection>();
        foreach (var p in projections)
        {
            if (string.IsNullOrEmpty(p.SportType) || !excludedSportTypes.Contains(p.SportType))
                included.Add(p.Id);
            else
                excluded.Add(p);
        }

        await QueueActivityIdsAsync(included, serviceBusClient, queueName);
        await MarkExcludedActivitiesAsync(activitiesClient, excluded, stage);
    }

    private static async Task MarkExcludedActivitiesAsync(
        CollectionClient<Activity> activitiesClient,
        List<ActivityIdProjection> excluded,
        ActivitySyncStage stage)
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
            await activitiesClient.PatchDocument(
                p.Id,
                new PartitionKey(p.UserId),
                [PatchOperation.Set("/processingStatus", next)]);
        }
    }

    private sealed class ActivityIdProjection
    {
        [JsonPropertyName("id")]
        public required string Id { get; init; }
        [JsonPropertyName("userId")]
        public required string UserId { get; init; }
        [JsonPropertyName("sportType")]
        public string? SportType { get; init; }
        [JsonPropertyName("processingStatus")]
        public ActivityProcessingStatus? ProcessingStatus { get; init; }
    }

    internal static QueryDefinition BuildActivityProjectionsQueryDefinition(string? userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
            return new QueryDefinition("SELECT c.id, c.userId, c.sportType, c.processingStatus FROM c");

        return new QueryDefinition("SELECT c.id, c.userId, c.sportType, c.processingStatus FROM c WHERE c.userId = @userId")
            .WithParameter("@userId", userId);
    }

    internal static QueryDefinition BuildActivityIdsQueryDefinition(string? userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
        {
            return new QueryDefinition("SELECT VALUE c.id FROM c");
        }

        return new QueryDefinition("SELECT VALUE c.id FROM c WHERE c.userId = @userId")
            .WithParameter("@userId", userId);
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
