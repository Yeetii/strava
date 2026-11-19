using API.Endpoints.Backend;
using Microsoft.AspNetCore.Http;

namespace Backend.Tests;

public class QueueAllVisitedAreasJobsTests
{
    [Fact]
    public void GetUserIdFilter_ReturnsUserIdWhenPresent()
    {
        var userId = QueueAllVisitedAreasJobs.GetUserIdFilter(CreateContext("?userId=11908635"));

        Assert.Equal("11908635", userId);
    }

    [Fact]
    public void GetUserIdFilter_ReturnsNullWhenMissing()
    {
        var userId = QueueAllVisitedAreasJobs.GetUserIdFilter(CreateContext(""));

        Assert.Null(userId);
    }

    private static HttpContext CreateContext(string query)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(query);
        return context;
    }
}
