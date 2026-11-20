using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;

namespace Stigar.Core.Services;

public enum ActivitySyncStage
{
    SummitedPeaks,
    VisitedPaths,
    VisitedAreas,
}

public class UserSyncStatusService(IUserRepository users, IActivityRepository activities)
{
    public static StravaSyncStatus CreateDefaultStatus() => new()
    {
        TotalActivitiesOnStrava = null,
        SyncedActivities = 0,
        ProcessedActivities = new StravaProcessedActivityCounts(),
        UpdatedAtUtc = DateTime.UtcNow,
    };

    public async Task IncrementSyncedActivities(string userId, int delta, CancellationToken cancellationToken = default)
    {
        if (delta <= 0)
            return;

        await PatchUserSyncStatus(userId, status =>
        {
            status.SyncedActivities += delta;
            status.UpdatedAtUtc = DateTime.UtcNow;
            return true;
        }, cancellationToken);
    }

    public async Task IncrementKnownTotalActivitiesOnStrava(string userId, int delta, CancellationToken cancellationToken = default)
    {
        if (delta <= 0)
            return;

        await PatchUserSyncStatus(userId, status =>
        {
            status.TotalActivitiesOnStrava = (status.TotalActivitiesOnStrava ?? 0) + delta;
            status.UpdatedAtUtc = DateTime.UtcNow;
            return true;
        }, cancellationToken);
    }

    public async Task SetTotalActivitiesOnStravaAtLeast(string userId, int count, CancellationToken cancellationToken = default)
    {
        await PatchUserSyncStatus(userId, status =>
        {
            if (status.TotalActivitiesOnStrava.HasValue && status.TotalActivitiesOnStrava.Value >= count)
                return false;

            status.TotalActivitiesOnStrava = count;
            status.UpdatedAtUtc = DateTime.UtcNow;
            return true;
        }, cancellationToken);
    }

    public async Task<bool> TryMarkActivityStageProcessed(string userId, string activityId, ActivitySyncStage stage, long epoch, CancellationToken cancellationToken = default)
    {
        var activity = await activities.GetById(activityId, cancellationToken)
            ?? throw new InvalidOperationException($"Activity {activityId} for user {userId} was not found.");

        if (IsStageProcessed(activity.ProcessingStatus, stage, epoch))
            return false;

        var nextStatus = CloneActivityProcessingStatus(activity.ProcessingStatus);
        SetStageProcessed(nextStatus, stage, epoch, DateTime.UtcNow);
        nextStatus.EnqueuedAtUtc = null;

        await activities.UpdateProcessingStatus(activity.Id, nextStatus, cancellationToken);
        await IncrementProcessedActivities(userId, stage, cancellationToken);
        return true;
    }

    private async Task IncrementProcessedActivities(string userId, ActivitySyncStage stage, CancellationToken cancellationToken)
    {
        await PatchUserSyncStatus(userId, status =>
        {
            switch (stage)
            {
                case ActivitySyncStage.SummitedPeaks:
                    status.ProcessedActivities.SummitedPeaks++;
                    break;
                case ActivitySyncStage.VisitedPaths:
                    status.ProcessedActivities.VisitedPaths++;
                    break;
                case ActivitySyncStage.VisitedAreas:
                    status.ProcessedActivities.VisitedAreas++;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(stage), stage, null);
            }

            status.UpdatedAtUtc = DateTime.UtcNow;
            return true;
        }, cancellationToken);
    }

    private async Task PatchUserSyncStatus(string userId, Func<StravaSyncStatus, bool> mutate, CancellationToken cancellationToken)
    {
        var user = await users.GetById(userId, cancellationToken)
            ?? throw new InvalidOperationException($"User {userId} was not found.");

        var nextStatus = CloneSyncStatus(user.SyncStatus);
        if (!mutate(nextStatus))
            return;

        user.SyncStatus = nextStatus;
        await users.Upsert(user, cancellationToken);
    }

    private static StravaSyncStatus CloneSyncStatus(StravaSyncStatus? current)
    {
        var status = current ?? CreateDefaultStatus();
        return new StravaSyncStatus
        {
            TotalActivitiesOnStrava = status.TotalActivitiesOnStrava,
            SyncedActivities = status.SyncedActivities,
            UpdatedAtUtc = status.UpdatedAtUtc,
            ProcessedActivities = new StravaProcessedActivityCounts
            {
                SummitedPeaks = status.ProcessedActivities?.SummitedPeaks ?? 0,
                VisitedPaths = status.ProcessedActivities?.VisitedPaths ?? 0,
                VisitedAreas = status.ProcessedActivities?.VisitedAreas ?? 0,
            }
        };
    }

    private static ActivityProcessingStatus CloneActivityProcessingStatus(ActivityProcessingStatus? current)
    {
        return new ActivityProcessingStatus
        {
            SummitedPeaksEpoch = current?.SummitedPeaksEpoch ?? 0,
            VisitedPathsEpoch = current?.VisitedPathsEpoch ?? 0,
            VisitedAreasEpoch = current?.VisitedAreasEpoch ?? 0,
            SportTypeExcluded = current?.SportTypeExcluded ?? false,
            SummitedPeaksDoneAtUtc = current?.SummitedPeaksDoneAtUtc,
            VisitedPathsDoneAtUtc = current?.VisitedPathsDoneAtUtc,
            VisitedAreasDoneAtUtc = current?.VisitedAreasDoneAtUtc,
            LastUpdatedAtUtc = current?.LastUpdatedAtUtc,
            LastProcessingError = current?.LastProcessingError,
            EnqueuedAtUtc = current?.EnqueuedAtUtc,
        };
    }

    private static bool IsStageProcessed(ActivityProcessingStatus? status, ActivitySyncStage stage, long epoch)
    {
        // Unversioned (manual) jobs still treat an already-completed stage as done.
        if (epoch <= 0)
            return GetStageEpoch(status, stage) > 0;

        return GetStageEpoch(status, stage) >= epoch;
    }

    private static long GetStageEpoch(ActivityProcessingStatus? status, ActivitySyncStage stage)
        => stage switch
        {
            ActivitySyncStage.SummitedPeaks => status?.SummitedPeaksEpoch ?? 0,
            ActivitySyncStage.VisitedPaths => status?.VisitedPathsEpoch ?? 0,
            ActivitySyncStage.VisitedAreas => status?.VisitedAreasEpoch ?? 0,
            _ => throw new ArgumentOutOfRangeException(nameof(stage), stage, null),
        };

    private static void SetStageProcessed(ActivityProcessingStatus status, ActivitySyncStage stage, long epoch, DateTime processedAtUtc)
    {
        switch (stage)
        {
            case ActivitySyncStage.SummitedPeaks:
                status.SummitedPeaksEpoch = epoch;
                status.SummitedPeaksDoneAtUtc = processedAtUtc;
                break;
            case ActivitySyncStage.VisitedPaths:
                status.VisitedPathsEpoch = epoch;
                status.VisitedPathsDoneAtUtc = processedAtUtc;
                break;
            case ActivitySyncStage.VisitedAreas:
                status.VisitedAreasEpoch = epoch;
                status.VisitedAreasDoneAtUtc = processedAtUtc;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(stage), stage, null);
        }

        status.LastUpdatedAtUtc = processedAtUtc;
        status.LastProcessingError = null;
    }
}
