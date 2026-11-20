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
    /// polls PostgreSQL for activities with pending processing stages before enqueueing the
    /// per-activity work. A stage is pending when its stored epoch is behind the current epoch
    /// for that job type; bumping an epoch marks the type for rediscovery.
    /// </summary>
    public class QueueActivityJobs(
        IBackgroundJobClient jobs,
        IActivityRepository activities,
        IBackendAdminQueries queries,
        IJobEpochService epochs) : IQueueActivityJobs
    {
        private const int PollBatchSize = 4000;

        public async Task Run(
            IReadOnlyList<Activity> updatedActivities,
            long summitsEpoch,
            long pathsEpoch,
            long areasEpoch,
            CancellationToken cancellationToken)
        {
            var enqueuedIds = new List<string>();
            foreach (var activity in updatedActivities)
            {
                var queued = false;
                if (ShouldQueueSummits(activity, summitsEpoch))
                {
                    jobs.EnqueueSummits(ActivityIdJob.From(activity.Id, summitsEpoch));
                    queued = true;
                }
                if (ShouldQueueVisitedPaths(activity, pathsEpoch))
                {
                    jobs.EnqueueVisitedPaths(ActivityIdJob.From(activity.Id, pathsEpoch));
                    queued = true;
                }
                if (ShouldQueueVisitedAreas(activity, areasEpoch))
                {
                    jobs.EnqueueVisitedAreas(ActivityIdJob.From(activity.Id, areasEpoch));
                    queued = true;
                }

                await MarkExcludedIfNeededAsync(activity, summitsEpoch, pathsEpoch, areasEpoch, cancellationToken);
                if (queued)
                    enqueuedIds.Add(activity.Id);
            }

            if (enqueuedIds.Count > 0)
                await queries.MarkActivitiesEnqueued(enqueuedIds, DateTime.UtcNow, cancellationToken);
        }

        /// <summary>Entry point for the Hangfire recurring polling job (see Program.cs).</summary>
        public async Task PollAndQueueAsync(CancellationToken cancellationToken = default)
        {
            var summitsEpoch = await epochs.GetAsync(JobEpochType.ActivitySummits, cancellationToken);
            var pathsEpoch = await epochs.GetAsync(JobEpochType.ActivityVisitedPaths, cancellationToken);
            var areasEpoch = await epochs.GetAsync(JobEpochType.ActivityVisitedAreas, cancellationToken);

            var ids = await queries.GetActivityIdsNeedingProcessing(
                summitsEpoch, pathsEpoch, areasEpoch, PollBatchSize, cancellationToken);
            if (ids.Count == 0)
                return;

            var activitiesToQueue = await activities.GetByIds(ids, cancellationToken);
            if (activitiesToQueue.Count > 0)
                await Run(activitiesToQueue, summitsEpoch, pathsEpoch, areasEpoch, cancellationToken);
        }

        private async Task MarkExcludedIfNeededAsync(
            Activity activity,
            long summitsEpoch,
            long pathsEpoch,
            long areasEpoch,
            CancellationToken cancellationToken)
        {
            var sportType = activity.SportType;
            var current = activity.ProcessingStatus;

            bool markPeaks = ActivityTypeFilters.ExcludedFromPeaks.Contains(sportType)
                             && (current?.SummitedPeaksEpoch ?? 0) < summitsEpoch;
            bool markPaths = ActivityTypeFilters.ExcludedFromPaths.Contains(sportType)
                             && (current?.VisitedPathsEpoch ?? 0) < pathsEpoch;
            bool markAreas = ActivityTypeFilters.ExcludedFromAreas.Contains(sportType)
                             && (current?.VisitedAreasEpoch ?? 0) < areasEpoch;

            if (!markPeaks && !markPaths && !markAreas)
                return;

            var now = DateTime.UtcNow;
            var next = new ActivityProcessingStatus
            {
                SummitedPeaksEpoch = markPeaks ? long.MaxValue : current?.SummitedPeaksEpoch ?? 0,
                VisitedPathsEpoch = markPaths ? long.MaxValue : current?.VisitedPathsEpoch ?? 0,
                VisitedAreasEpoch = markAreas ? long.MaxValue : current?.VisitedAreasEpoch ?? 0,
                SummitedPeaksDoneAtUtc = markPeaks ? now : current?.SummitedPeaksDoneAtUtc,
                VisitedPathsDoneAtUtc = markPaths ? now : current?.VisitedPathsDoneAtUtc,
                VisitedAreasDoneAtUtc = markAreas ? now : current?.VisitedAreasDoneAtUtc,
                LastUpdatedAtUtc = now,
                LastProcessingError = current?.LastProcessingError,
                SportTypeExcluded = true,
                EnqueuedAtUtc = current?.EnqueuedAtUtc,
            };

            await activities.UpdateProcessingStatus(activity.Id, next, cancellationToken);
        }

        internal static bool ShouldQueueSummits(Activity activity, long epoch)
            => (activity.ProcessingStatus?.SummitedPeaksEpoch ?? 0) < epoch
            && !ActivityTypeFilters.ExcludedFromPeaks.Contains(activity.SportType);

        internal static bool ShouldQueueVisitedPaths(Activity activity, long epoch)
            => (activity.ProcessingStatus?.VisitedPathsEpoch ?? 0) < epoch
            && !ActivityTypeFilters.ExcludedFromPaths.Contains(activity.SportType);

        internal static bool ShouldQueueVisitedAreas(Activity activity, long epoch)
            => (activity.ProcessingStatus?.VisitedAreasEpoch ?? 0) < epoch
            && !ActivityTypeFilters.ExcludedFromAreas.Contains(activity.SportType);
    }
}
