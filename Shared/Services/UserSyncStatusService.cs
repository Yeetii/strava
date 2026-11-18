using Shared.Models;
using Shared.Services.Postgres;

namespace Shared.Services;

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

    public async Task<bool> TryMarkActivityStageProcessed(string userId, string activityId, ActivitySyncStage stage, CancellationToken cancellationToken = default)
    {
        var activity = await activities.GetById(activityId, cancellationToken)
            ?? throw new InvalidOperationException($"Activity {activityId} for user {userId} was not found.");

        if (IsStageProcessed(activity.ProcessingStatus, stage))
            return false;

        var nextStatus = CloneActivityProcessingStatus(activity.ProcessingStatus);
        SetStageProcessed(nextStatus, stage, DateTime.UtcNow);

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
            SummitedPeaks = current?.SummitedPeaks ?? false,
            VisitedPaths = current?.VisitedPaths ?? false,
            VisitedAreas = current?.VisitedAreas ?? false,
            SportTypeExcluded = current?.SportTypeExcluded ?? false,
            SummitedPeaksDoneAtUtc = current?.SummitedPeaksDoneAtUtc,
            VisitedPathsDoneAtUtc = current?.VisitedPathsDoneAtUtc,
            VisitedAreasDoneAtUtc = current?.VisitedAreasDoneAtUtc,
            LastUpdatedAtUtc = current?.LastUpdatedAtUtc,
            LastProcessingError = current?.LastProcessingError,
        };
    }

    private static bool IsStageProcessed(ActivityProcessingStatus? status, ActivitySyncStage stage)
    {
        return stage switch
        {
            ActivitySyncStage.SummitedPeaks => status?.SummitedPeaks == true,
            ActivitySyncStage.VisitedPaths => status?.VisitedPaths == true,
            ActivitySyncStage.VisitedAreas => status?.VisitedAreas == true,
            _ => throw new ArgumentOutOfRangeException(nameof(stage), stage, null),
        };
    }

    private static void SetStageProcessed(ActivityProcessingStatus status, ActivitySyncStage stage, DateTime processedAtUtc)
    {
        switch (stage)
        {
            case ActivitySyncStage.SummitedPeaks:
                status.SummitedPeaks = true;
                status.SummitedPeaksDoneAtUtc = processedAtUtc;
                break;
            case ActivitySyncStage.VisitedPaths:
                status.VisitedPaths = true;
                status.VisitedPathsDoneAtUtc = processedAtUtc;
                break;
            case ActivitySyncStage.VisitedAreas:
                status.VisitedAreas = true;
                status.VisitedAreasDoneAtUtc = processedAtUtc;
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(stage), stage, null);
        }

        status.LastUpdatedAtUtc = processedAtUtc;
        status.LastProcessingError = null;
    }
}
