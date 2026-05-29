namespace Backend.Tests;

public class QueueActivityCollectionJobsTests
{
    [Fact]
    public void BuildQueueAllMessageId_ReturnsSameId_ForSameQueueAndActivity()
    {
        var first = QueueActivityCollectionJobs.BuildQueueAllMessageId("calculateVisitedPathsJobs", "12345");
        var second = QueueActivityCollectionJobs.BuildQueueAllMessageId("calculateVisitedPathsJobs", "12345");

        Assert.Equal(first, second);
    }

    [Fact]
    public void BuildQueueAllMessageId_ReturnsDifferentId_ForDifferentActivity()
    {
        var first = QueueActivityCollectionJobs.BuildQueueAllMessageId("calculateVisitedPathsJobs", "12345");
        var second = QueueActivityCollectionJobs.BuildQueueAllMessageId("calculateVisitedPathsJobs", "67890");

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void BuildQueueAllMessageId_NormalizesQueueNameCase()
    {
        var first = QueueActivityCollectionJobs.BuildQueueAllMessageId("CalculateVisitedPathsJobs", "12345");
        var second = QueueActivityCollectionJobs.BuildQueueAllMessageId("calculatevisitedpathsjobs", "12345");

        Assert.Equal(first, second);
    }
}
