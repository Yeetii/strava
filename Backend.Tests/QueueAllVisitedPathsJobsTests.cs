namespace Backend.Tests;

public class QueueAllVisitedPathsJobsTests
{
    [Fact]
    public void GetUserIdFilter_ReturnsUserIdWhenPresent()
    {
        var userId = QueueAllVisitedPathsJobs.GetUserIdFilter(new Uri("http://localhost:1234/api/QueueAllVisitedPathsJobs?userId=11908635"));

        Assert.Equal("11908635", userId);
    }

    [Fact]
    public void GetUserIdFilter_ReturnsNullWhenMissing()
    {
        var userId = QueueAllVisitedPathsJobs.GetUserIdFilter(new Uri("http://localhost:1234/api/QueueAllVisitedPathsJobs"));

        Assert.Null(userId);
    }
}
