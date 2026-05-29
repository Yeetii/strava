using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Services;

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
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.Add(spacing * index)
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

    internal static QueryDefinition BuildActivityIdsQueryDefinition(string? userId)
    {
        if (string.IsNullOrWhiteSpace(userId))
        {
            return new QueryDefinition("SELECT VALUE c.id FROM c");
        }

        return new QueryDefinition("SELECT VALUE c.id FROM c WHERE c.userId = @userId")
            .WithParameter("@userId", userId);
    }
}
