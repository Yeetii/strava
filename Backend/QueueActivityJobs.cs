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
                var queueTasks = new List<Task>(capacity: 3);
                if (ShouldQueueSummits(activity))
                    queueTasks.Add(summitsSender.SendMessageAsync(new ServiceBusMessage(activity.Id)));

                if (ShouldQueueVisitedPaths(activity))
                    queueTasks.Add(pathsSender.SendMessageAsync(new ServiceBusMessage(activity.Id)));

                if (ShouldQueueVisitedAreas(activity))
                    queueTasks.Add(areasSender.SendMessageAsync(new ServiceBusMessage(activity.Id)));

                await Task.WhenAll(queueTasks);
            }
        }

        internal static bool ShouldQueueSummits(Activity activity)
            => activity.ProcessingStatus?.SummitedPeaks != true;

        internal static bool ShouldQueueVisitedPaths(Activity activity)
            => activity.ProcessingStatus?.VisitedPaths != true;

        internal static bool ShouldQueueVisitedAreas(Activity activity)
            => activity.ProcessingStatus?.VisitedAreas != true;
    }
}
