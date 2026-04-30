using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Shared.Constants;
using Shared.Models;

namespace Backend
{
    public class QueueActivityJobs(ServiceBusClient serviceBusClient)
    {
        [Function(nameof(QueueActivityJobs))]
        public async Task Run(
            [CosmosDBTrigger(
            databaseName: DatabaseConfig.CosmosDb,
            containerName: DatabaseConfig.ActivitiesContainer,
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "activityJobs",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<Activity> updatedActivities)
        {
            var summitsSender = serviceBusClient.CreateSender(ServiceBusConfig.CalculateSummitsJobs);
            var pathsSender = serviceBusClient.CreateSender(ServiceBusConfig.CalculateVisitedPathsJobs);
            var areasSender = serviceBusClient.CreateSender(ServiceBusConfig.CalculateVisitedAreasJobs);

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
