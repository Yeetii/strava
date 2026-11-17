using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker.Http;

namespace Backend.Tests;

public class SelfHostedQueryTests
{
    [Fact]
    public void Query_MissingKey_ReturnsNull()
    {
        var request = new HttpRequestData(CreateContext());

        Assert.Null(request.Query["zoom"]);
    }

    [Fact]
    public void Query_PresentKey_ReturnsValue()
    {
        var context = CreateContext();
        context.Request.QueryString = new QueryString("?zoom=12");
        var request = new HttpRequestData(context);

        Assert.Equal("12", request.Query["zoom"]);
    }

    [Fact]
    public void Query_MissingKey_IsCaseInsensitiveLookup()
    {
        var context = CreateContext();
        context.Request.QueryString = new QueryString("?Zoom=12");
        var request = new HttpRequestData(context);

        Assert.Equal("12", request.Query["zoom"]);
    }

    private static DefaultHttpContext CreateContext()
    {
        var context = new DefaultHttpContext();
        context.Request.Scheme = "http";
        context.Request.Host = new HostString("localhost");
        return context;
    }
}
