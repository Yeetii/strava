using Shared.Models;

namespace Backend.Tests;

public class QueueActivityJobsTests
{
    [Fact]
    public void ShouldQueueActivity_ReturnsTrue_WhenProcessingStatusIsMissing()
    {
        var activity = CreateActivity();

        Assert.True(QueueActivityJobs.ShouldQueueActivity(activity));
    }

    [Fact]
    public void ShouldQueueActivity_ReturnsTrue_WhenNoStagesHaveRunYet()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus();

        Assert.True(QueueActivityJobs.ShouldQueueActivity(activity));
    }

    [Fact]
    public void ShouldQueueActivity_ReturnsFalse_WhenAnyStageHasAlreadyRun()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus { VisitedAreas = true };

        Assert.False(QueueActivityJobs.ShouldQueueActivity(activity));
    }

    private static Activity CreateActivity() => new()
    {
        Id = "activity-1",
        UserId = "user-1",
        Name = "Test Activity",
        SportType = SportTypes.RUN,
        StartDate = DateTime.UtcNow,
        StartDateLocal = DateTime.UtcNow,
    };
}