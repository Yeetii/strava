namespace Backend.Tests;

public class QueueAllVisitedAreasJobsTests
{
    [Fact]
    public void GetUserIdFilter_ReturnsUserIdWhenPresent()
    {
        var userId = QueueAllVisitedAreasJobs.GetUserIdFilter(new Uri("http://localhost:1234/api/QueueAllVisitedAreasJobs?userId=11908635"));

        Assert.Equal("11908635", userId);
    }

    [Fact]
    public void GetUserIdFilter_ReturnsNullWhenMissing()
    {
        var userId = QueueAllVisitedAreasJobs.GetUserIdFilter(new Uri("http://localhost:1234/api/QueueAllVisitedAreasJobs"));

        Assert.Null(userId);
    }
}