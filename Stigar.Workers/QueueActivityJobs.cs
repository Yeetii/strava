using Hangfire;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Workers
{
    /// <summary>
    /// <see cref="PollAndQueueAsync"/> is registered as a Hangfire recurring job by Program.cs and
    /// polls PostgreSQL for activities with pending processing stages before running the same
    /// per-activity enqueue/mark-excluded logic as before.
    /// </summary>
    public class QueueActivityJobs(
        IBackgroundJobClient jobs,
        IActivityRepository activities,
        IBackendAdminQueries queries) : IQueueActivityJobs
    {
        private const int PollBatchSize = 500;

        public async Task Run(
            IReadOnlyList<Activity> updatedActivities,
            CancellationToken cancellationToken)
        {
            foreach (var activity in updatedActivities)
            {
                var idJob = ActivityIdJob.From(activity.Id);
                if (ShouldQueueSummits(activity))
                    jobs.EnqueueSummits(idJob);
                if (ShouldQueueVisitedPaths(activity))
                    jobs.EnqueueVisitedPaths(idJob);
                if (ShouldQueueVisitedAreas(activity))
                    jobs.EnqueueVisitedAreas(idJob);

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
