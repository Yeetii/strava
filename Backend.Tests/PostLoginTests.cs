using API.Endpoints;
using Shared.Models;
using Shared.Services;

namespace Backend.Tests;

public class PostLoginTests
{
    [Fact]
    public void ShouldQueueInitialActivitiesFetch_ReturnsTrue_WhenUserDoesNotExist()
    {
        Assert.True(PostLogin.ShouldQueueInitialActivitiesFetch(null));
    }

    [Fact]
    public void ShouldQueueInitialActivitiesFetch_ReturnsTrue_WhenExistingUserHasNoSyncProgress()
    {
        var user = CreateUser(syncStatus: null);

        Assert.True(PostLogin.ShouldQueueInitialActivitiesFetch(user));
    }

    [Fact]
    public void ShouldQueueInitialActivitiesFetch_ReturnsFalse_WhenExistingUserHasSyncedActivities()
    {
        var syncStatus = UserSyncStatusService.CreateDefaultStatus();
        syncStatus.SyncedActivities = 1;
        var user = CreateUser(syncStatus);

        Assert.False(PostLogin.ShouldQueueInitialActivitiesFetch(user));
    }

    [Fact]
    public void ShouldQueueInitialActivitiesFetch_ReturnsFalse_WhenExistingUserHasKnownTotalActivities()
    {
        var syncStatus = UserSyncStatusService.CreateDefaultStatus();
        syncStatus.TotalActivitiesOnStrava = 0;
        var user = CreateUser(syncStatus);

        Assert.False(PostLogin.ShouldQueueInitialActivitiesFetch(user));
    }

    private static User CreateUser(StravaSyncStatus? syncStatus) => new()
    {
        Id = "123",
        SyncStatus = syncStatus,
    };
}
