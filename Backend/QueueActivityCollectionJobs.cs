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
        string? userId = null)
    {
        var projections = await activitiesClient.ExecuteQueryAsync<ActivityIdProjection>(
            BuildActivityProjectionsQueryDefinition(userId));
        var activityIds = projections
            .Where(p => string.IsNullOrEmpty(p.SportType) || !excludedSportTypes.Contains(p.SportType))
            .Select(p => p.Id);
        await QueueActivityIdsAsync(activityIds, serviceBusClient, queueName);
    }

    private sealed class ActivityIdProjection
    {
        [JsonPropertyName("id")]
        public required string Id { get; init; }
        [JsonPropertyName("sportType")]
        public string? SportType { get; init; }
    }

    internal static QueryDefinition BuildActivityProjectionsQueryDefinition(string? userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
            return new QueryDefinition("SELECT c.id, c.sportType FROM c");

        return new QueryDefinition("SELECT c.id, c.sportType FROM c WHERE c.userId = @userId")
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
