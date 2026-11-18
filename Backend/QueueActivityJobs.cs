using Microsoft.Azure.Functions.Worker;
using Shared.Constants;
using Shared.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Admin;

namespace Backend
{
    /// <summary>
    /// Replacement for the activities Cosmos change feed. The CosmosDBTrigger handler is gone;
    /// <see cref="PollAndQueueAsync"/> is registered as a Hangfire recurring job by Program.cs and
    /// polls PostgreSQL for activities with pending processing stages before running the same
    /// per-activity enqueue/mark-excluded logic as before.
    /// </summary>
    public class QueueActivityJobs(
        QueueClient queueClient,
        IActivityRepository activities,
        IBackendAdminQueries queries) : IQueueActivityJobs
    {
        private const int PollBatchSize = 500;

        public async Task Run(
            IReadOnlyList<Activity> updatedActivities,
            CancellationToken cancellationToken)
        {
            var summitsSender = queueClient.CreateSender(QueueConfig.CalculateSummitsJobs);
            var pathsSender = queueClient.CreateSender(QueueConfig.CalculateVisitedPathsJobs);
            var areasSender = queueClient.CreateSender(QueueConfig.CalculateVisitedAreasJobs);

            foreach (var activity in updatedActivities)
            {
                var queueTasks = new List<Task>(capacity: 3);
                if (ShouldQueueSummits(activity))
                    queueTasks.Add(summitsSender.SendMessageAsync(new QueueMessage(activity.Id)));

                if (ShouldQueueVisitedPaths(activity))
                    queueTasks.Add(pathsSender.SendMessageAsync(new QueueMessage(activity.Id)));

                if (ShouldQueueVisitedAreas(activity))
                    queueTasks.Add(areasSender.SendMessageAsync(new QueueMessage(activity.Id)));

                await Task.WhenAll(queueTasks);
                await MarkExcludedIfNeededAsync(activity, cancellationToken);
            }
        }

        /// <summary>Entry point for the Hangfire recurring polling job (see Program.cs).</summary>
        public async Task PollAndQueueAsync(CancellationToken cancellationToken = default)
        {
            var ids = await queries.GetActivityIdsNeedingProcessing(PollBatchSize, cancellationToken);
            if (ids.Count == 0)
                return;

            var activitiesToQueue = await activities.GetByIds(ids, cancellationToken);
            if (activitiesToQueue.Count > 0)
                await Run(activitiesToQueue, cancellationToken);
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

            await activities.UpdateProcessingStatus(activity.Id, next, cancellationToken);
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
