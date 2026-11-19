using Moq;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;

namespace Stigar.Workers.Tests;

public class UserSyncStatusServiceTests
{
    [Fact]
    public async Task TryMarkActivityStageProcessed_SetsStageTimestampAndLastUpdated()
    {
        var userId = "user-1";
        var activity = new Activity
        {
            Id = "activity-1",
            UserId = userId,
            Name = "Morning Run",
            SportType = SportTypes.RUN,
            StartDate = DateTime.UtcNow,
            StartDateLocal = DateTime.UtcNow
        };
        var user = new Stigar.Core.Models.User
        {
            Id = userId,
            SyncStatus = UserSyncStatusService.CreateDefaultStatus()
        };

        var activitiesRepository = new Mock<IActivityRepository>();
        var usersRepository = new Mock<IUserRepository>();

        activitiesRepository
            .Setup(r => r.GetById(activity.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);
        usersRepository
            .Setup(r => r.GetById(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(user);

        var service = new UserSyncStatusService(usersRepository.Object, activitiesRepository.Object);

        var changed = await service.TryMarkActivityStageProcessed(userId, activity.Id, ActivitySyncStage.SummitedPeaks);

        Assert.True(changed);
        activitiesRepository.Verify(r => r.UpdateProcessingStatus(
            activity.Id,
            It.Is<ActivityProcessingStatus?>(status =>
                status != null
                && status.SummitedPeaks
                && status.SummitedPeaksDoneAtUtc.HasValue
                && status.LastUpdatedAtUtc.HasValue
                && status.SummitedPeaksDoneAtUtc == status.LastUpdatedAtUtc),
            It.IsAny<CancellationToken>()), Times.Once);
        usersRepository.Verify(r => r.Upsert(
            It.Is<User>(u => u.SyncStatus != null && u.SyncStatus.ProcessedActivities.SummitedPeaks == 1),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TryMarkActivityStageProcessed_WhenAlreadyProcessed_DoesNotPatch()
    {
        var userId = "user-1";
        var activity = new Activity
        {
            Id = "activity-1",
            UserId = userId,
            Name = "Morning Run",
            SportType = SportTypes.RUN,
            StartDate = DateTime.UtcNow,
            StartDateLocal = DateTime.UtcNow,
            ProcessingStatus = new ActivityProcessingStatus
            {
                SummitedPeaks = true,
                SummitedPeaksDoneAtUtc = DateTime.UtcNow
            }
        };

        var activitiesRepository = new Mock<IActivityRepository>();
        activitiesRepository
            .Setup(r => r.GetById(activity.Id, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);

        var service = new UserSyncStatusService(
            new Mock<IUserRepository>().Object,
            activitiesRepository.Object);

        var changed = await service.TryMarkActivityStageProcessed(userId, activity.Id, ActivitySyncStage.SummitedPeaks);

        Assert.False(changed);
        activitiesRepository.Verify(r => r.UpdateProcessingStatus(
            It.IsAny<string>(),
            It.IsAny<ActivityProcessingStatus?>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }
}
