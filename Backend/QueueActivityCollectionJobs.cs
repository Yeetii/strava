using Azure.Messaging.ServiceBus;
using Shared.Models;
using Shared.Services;

namespace Backend;

internal static class QueueActivityCollectionJobs
{
    private static readonly TimeSpan MessageSpacing = TimeSpan.FromSeconds(5);

    public static async Task QueueAllActivitiesAsync(
        CollectionClient<Activity> activitiesClient,
        ServiceBusClient serviceBusClient,
        string queueName)
    {
        var activities = (await activitiesClient.FetchWholeCollection()).ToList();
        var sender = serviceBusClient.CreateSender(queueName);

        for (var index = 0; index < activities.Count; index++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage(activities[index].Id)
            {
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.Add(MessageSpacing * index)
            });
        }
    }
}
