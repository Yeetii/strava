using Shared.Models;

namespace Backend.Tests;

public class QueueActivityJobsTests
{
    [Fact]
    public void ShouldQueueAnyStage_ReturnsTrue_WhenProcessingStatusIsMissing()
    {
        var activity = CreateActivity();

        Assert.True(ShouldQueueAnyStage(activity));
    }

    [Fact]
    public void ShouldQueueAnyStage_ReturnsTrue_WhenNoStagesHaveRunYet()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus();

        Assert.True(ShouldQueueAnyStage(activity));
    }

    [Fact]
    public void ShouldQueueAnyStage_ReturnsTrue_WhenAnyStageHasAlreadyRunButOthersRemain()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus { VisitedAreas = true };

        Assert.True(ShouldQueueAnyStage(activity));
    }

    [Fact]
    public void ShouldQueueAnyStage_ReturnsFalse_WhenAllStagesHaveRun()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus
        {
            SummitedPeaks = true,
            VisitedPaths = true,
            VisitedAreas = true
        };

        Assert.False(ShouldQueueAnyStage(activity));
    }

    [Fact]
    public void ShouldQueueSummits_ReturnsFalse_WhenSummitsAlreadyProcessed()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus { SummitedPeaks = true };

        Assert.False(QueueActivityJobs.ShouldQueueSummits(activity));
        Assert.True(QueueActivityJobs.ShouldQueueVisitedPaths(activity));
        Assert.True(QueueActivityJobs.ShouldQueueVisitedAreas(activity));
    }

    [Fact]
    public void ShouldQueueVisitedPaths_ReturnsFalse_WhenVisitedPathsAlreadyProcessed()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus { VisitedPaths = true };

        Assert.True(QueueActivityJobs.ShouldQueueSummits(activity));
        Assert.False(QueueActivityJobs.ShouldQueueVisitedPaths(activity));
        Assert.True(QueueActivityJobs.ShouldQueueVisitedAreas(activity));
    }

    [Fact]
    public void ShouldQueueVisitedAreas_ReturnsFalse_WhenVisitedAreasAlreadyProcessed()
    {
        var activity = CreateActivity();
        activity.ProcessingStatus = new ActivityProcessingStatus { VisitedAreas = true };

        Assert.True(QueueActivityJobs.ShouldQueueSummits(activity));
        Assert.True(QueueActivityJobs.ShouldQueueVisitedPaths(activity));
        Assert.False(QueueActivityJobs.ShouldQueueVisitedAreas(activity));
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

    private static bool ShouldQueueAnyStage(Activity activity)
        => QueueActivityJobs.ShouldQueueSummits(activity)
            || QueueActivityJobs.ShouldQueueVisitedPaths(activity)
            || QueueActivityJobs.ShouldQueueVisitedAreas(activity);
}