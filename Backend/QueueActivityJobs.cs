using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend
{
    public class QueueActivityJobs(ServiceBusClient serviceBusClient, CollectionClient<Activity> activitiesCollection)
    {
        [Function(nameof(QueueActivityJobs))]
        public async Task Run(
            [CosmosDBTrigger(
            databaseName: DatabaseConfig.CosmosDb,
            containerName: DatabaseConfig.ActivitiesContainer,
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "activityJobs",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<Activity> updatedActivities,
            CancellationToken cancellationToken)
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
                await MarkExcludedIfNeededAsync(activity, cancellationToken);
            }
        }

        private async Task MarkExcludedIfNeededAsync(Activity activity, CancellationToken cancellationToken)
        {
            var sportType = activity.SportType;
            var current = activity.ProcessingStatus;

            bool markPeaks = ActivityTypeFilters.ExcludedFromPeaks.Contains(sportType)
                             && current?.SummitedPeaks != true;
            bool markPaths = ActivityTypeFilters.ExcludedFromPaths.Contains(sportType)
                             && current?.VisitedPaths != true;
            bool markAreas = ActivityTypeFilters.ExcludedFromAreas.Contains(sportType)
                             && current?.VisitedAreas != true;

            if (!markPeaks && !markPaths && !markAreas)
                return;

            var now = DateTime.UtcNow;
            var next = new ActivityProcessingStatus
            {
                SummitedPeaks = markPeaks || (current?.SummitedPeaks ?? false),
                VisitedPaths = markPaths || (current?.VisitedPaths ?? false),
                VisitedAreas = markAreas || (current?.VisitedAreas ?? false),
                SummitedPeaksDoneAtUtc = markPeaks ? now : current?.SummitedPeaksDoneAtUtc,
                VisitedPathsDoneAtUtc = markPaths ? now : current?.VisitedPathsDoneAtUtc,
                VisitedAreasDoneAtUtc = markAreas ? now : current?.VisitedAreasDoneAtUtc,
                LastUpdatedAtUtc = now,
                LastProcessingError = current?.LastProcessingError,
                SportTypeExcluded = true,
            };

            await activitiesCollection.PatchDocument(
                activity.Id,
                new PartitionKey(activity.UserId),
                [PatchOperation.Set("/processingStatus", next)],
                cancellationToken: cancellationToken);
        }

        internal static bool ShouldQueueSummits(Activity activity)
            => activity.ProcessingStatus?.SummitedPeaks != true
            && !ActivityTypeFilters.ExcludedFromPeaks.Contains(activity.SportType);

        internal static bool ShouldQueueVisitedPaths(Activity activity)
            => activity.ProcessingStatus?.VisitedPaths != true
            && !ActivityTypeFilters.ExcludedFromPaths.Contains(activity.SportType);

        internal static bool ShouldQueueVisitedAreas(Activity activity)
            => activity.ProcessingStatus?.VisitedAreas != true
            && !ActivityTypeFilters.ExcludedFromAreas.Contains(activity.SportType);
    }
}
