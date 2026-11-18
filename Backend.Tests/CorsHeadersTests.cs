using API.Utils;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker.Http;

namespace Backend.Tests;

public class CorsHeadersTests
{
    [Fact]
    public void Add_IncludesPrivateNetworkAndCredentials()
    {
        var httpContext = new DefaultHttpContext();
        httpContext.Request.Scheme = "https";
        httpContext.Request.Host = new HostString("localhost");
        httpContext.Request.Path = "/api/userSync";
        httpContext.Request.Headers["Origin"] = "https://stigar.erikmagnusson.com";
        httpContext.Request.Headers["Access-Control-Request-Private-Network"] = "true";

        var request = new HttpRequestData(httpContext);
        var response = request.CreateResponse();

        CorsHeaders.Add(request, response, "GET, POST, OPTIONS");

        Assert.True(response.Headers.TryGetValues("Access-Control-Allow-Origin", out var origins));
        Assert.Equal("https://stigar.erikmagnusson.com", Assert.Single(origins));

        Assert.True(response.Headers.TryGetValues("Access-Control-Allow-Private-Network", out var pna));
        Assert.Equal("true", Assert.Single(pna));

        Assert.True(response.Headers.TryGetValues("Access-Control-Allow-Credentials", out var creds));
        Assert.Equal("true", Assert.Single(creds));
    }
}
