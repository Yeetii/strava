using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Services;

namespace Backend;

internal static class QueueActivityCollectionJobs
{
    private static readonly TimeSpan MessageSpacing = TimeSpan.FromSeconds(5);

    public static async Task QueueAllActivitiesAsync(
        CollectionClient<Activity> activitiesClient,
        ServiceBusClient serviceBusClient,
        string queueName,
        string? userId = null)
    {
        var activityIds = (await activitiesClient.ExecuteQueryAsync<string>(BuildActivityIdsQueryDefinition(userId))).ToList();
        var sender = serviceBusClient.CreateSender(queueName);

        for (var index = 0; index < activityIds.Count; index++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage(activityIds[index])
            {
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.Add(MessageSpacing * index)
            });
        }
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
