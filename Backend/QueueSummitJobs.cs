using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Shared.Models;

namespace Backend
{
    public class QueueActivityJobs(ServiceBusClient serviceBusClient)
    {
        [Function(nameof(QueueActivityJobs))]
        public async Task Run(
            [CosmosDBTrigger(
            databaseName: "%CosmosDb%",
            containerName: "%ActivitiesContainer%",
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "activityJobs",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<Activity> updatedActivities)
        {
            var summitsSender = serviceBusClient.CreateSender("calculateSummitsJobs");
            var pathsSender = serviceBusClient.CreateSender("calculateVisitedPathsJobs");
            var areasSender = serviceBusClient.CreateSender("calculateVisitedAreasJobs");

            foreach (var activity in updatedActivities)
            {
                await Task.WhenAll(
                    summitsSender.SendMessageAsync(new ServiceBusMessage(activity.Id)),
                    pathsSender.SendMessageAsync(new ServiceBusMessage(activity.Id)),
                    areasSender.SendMessageAsync(new ServiceBusMessage(activity.Id))
                );
            }
        }
    }
}
