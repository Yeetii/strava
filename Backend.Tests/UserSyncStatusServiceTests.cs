using System.Net;
using System.Reflection;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Moq;
using Shared.Models;
using Shared.Services;

namespace Backend.Tests;

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
            StartDateLocal = DateTime.UtcNow,
            ETag = "activity-etag"
        };
        var user = new Shared.Models.User
        {
            Id = userId,
            SyncStatus = UserSyncStatusService.CreateDefaultStatus(),
            ETag = "user-etag"
        };

        var loggerFactory = LoggerFactory.Create(builder => builder.AddFilter(_ => false));
        var activitiesContainer = new Mock<Container>();
        var usersContainer = new Mock<Container>();

        activitiesContainer
            .Setup(c => c.ReadItemAsync<Activity>(activity.Id, It.IsAny<PartitionKey>(), null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<Activity>>(r => r.Resource == activity));

        IReadOnlyList<PatchOperation>? activityPatch = null;
        activitiesContainer
            .Setup(c => c.PatchItemAsync<Activity>(
                activity.Id,
                It.IsAny<PartitionKey>(),
                It.IsAny<IReadOnlyList<PatchOperation>>(),
                It.IsAny<PatchItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, PartitionKey, IReadOnlyList<PatchOperation>, PatchItemRequestOptions?, CancellationToken>((_, _, operations, _, _) => activityPatch = operations)
            .ReturnsAsync(Mock.Of<ItemResponse<Activity>>(r => r.Resource == activity));

        usersContainer
            .Setup(c => c.ReadItemAsync<Shared.Models.User>(userId, It.IsAny<PartitionKey>(), null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<Shared.Models.User>>(r => r.Resource == user));

        usersContainer
            .Setup(c => c.PatchItemAsync<Shared.Models.User>(
                userId,
                It.IsAny<PartitionKey>(),
                It.IsAny<IReadOnlyList<PatchOperation>>(),
                It.IsAny<PatchItemRequestOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<Shared.Models.User>>(r => r.Resource == user));

        var service = new UserSyncStatusService(
            new CollectionClient<Shared.Models.User>(usersContainer.Object, loggerFactory),
            new CollectionClient<Activity>(activitiesContainer.Object, loggerFactory));

        var changed = await service.TryMarkActivityStageProcessed(userId, activity.Id, ActivitySyncStage.SummitedPeaks);

        Assert.True(changed);
        var status = ExtractPatchedProcessingStatus(activityPatch);
        Assert.NotNull(status);
        Assert.True(status!.SummitedPeaks);
        Assert.NotNull(status.SummitedPeaksDoneAtUtc);
        Assert.NotNull(status.LastUpdatedAtUtc);
        Assert.Equal(status.SummitedPeaksDoneAtUtc, status.LastUpdatedAtUtc);
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
            ETag = "activity-etag",
            ProcessingStatus = new ActivityProcessingStatus
            {
                SummitedPeaks = true,
                SummitedPeaksDoneAtUtc = DateTime.UtcNow
            }
        };

        var loggerFactory = LoggerFactory.Create(builder => builder.AddFilter(_ => false));
        var activitiesContainer = new Mock<Container>();
        var usersContainer = new Mock<Container>();

        activitiesContainer
            .Setup(c => c.ReadItemAsync<Activity>(activity.Id, It.IsAny<PartitionKey>(), null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Mock.Of<ItemResponse<Activity>>(r => r.Resource == activity));

        var service = new UserSyncStatusService(
            new CollectionClient<Shared.Models.User>(usersContainer.Object, loggerFactory),
            new CollectionClient<Activity>(activitiesContainer.Object, loggerFactory));

        var changed = await service.TryMarkActivityStageProcessed(userId, activity.Id, ActivitySyncStage.SummitedPeaks);

        Assert.False(changed);
        activitiesContainer.Verify(c => c.PatchItemAsync<Activity>(
            It.IsAny<string>(),
            It.IsAny<PartitionKey>(),
            It.IsAny<IReadOnlyList<PatchOperation>>(),
            It.IsAny<PatchItemRequestOptions>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    private static ActivityProcessingStatus? ExtractPatchedProcessingStatus(IReadOnlyList<PatchOperation>? operations)
    {
        var operation = operations?.SingleOrDefault(op => op.Path == "/processingStatus");
        if (operation == null)
            return null;

        var valueProperty = operation.GetType().GetProperty("Value", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        return valueProperty?.GetValue(operation) as ActivityProcessingStatus;
    }
}
