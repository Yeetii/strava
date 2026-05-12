namespace Backend.Tests;

public class QueueAllSummitJobsTests
{
    [Fact]
    public void GetUserIdFilter_ReturnsUserIdWhenPresent()
    {
        var userId = QueueAllSummitJobs.GetUserIdFilter(new Uri("http://localhost:1234/api/QueueAllSummitJobs?userId=11908635"));

        Assert.Equal("11908635", userId);
    }

    [Fact]
    public void GetUserIdFilter_ReturnsNullWhenMissing()
    {
        var userId = QueueAllSummitJobs.GetUserIdFilter(new Uri("http://localhost:1234/api/QueueAllSummitJobs"));

        Assert.Null(userId);
    }
}