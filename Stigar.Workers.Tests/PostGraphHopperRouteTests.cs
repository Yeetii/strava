using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Configuration;
using Stigar.Api.Endpoints.Routing;

namespace Stigar.Workers.Tests;

public class PostGraphHopperRouteTests
{
    [Fact]
    public async Task Handle_InvalidJsonBody_ReturnsBadRequestWithoutCallingUpstream()
    {
        var handler = new RecordingHandler();
        var endpoint = new PostGraphHopperRoute(new HttpClient(handler), CreateConfiguration());
        var context = CreateContext("<script>alert(1)</script>");

        var result = await endpoint.Handle(context, CancellationToken.None);

        Assert.Equal(StatusCodes.Status400BadRequest,
            Assert.IsAssignableFrom<IStatusCodeHttpResult>(result).StatusCode);
        Assert.Equal(0, handler.CallCount);
    }

    [Fact]
    public async Task Handle_NonJsonUpstreamResponse_IsNotRelayedVerbatim()
    {
        var handler = new RecordingHandler { ResponseBody = "<script>alert(1)</script>" };
        var endpoint = new PostGraphHopperRoute(new HttpClient(handler), CreateConfiguration());
        var context = CreateContext("{\"hello\":\"world\"}");

        var result = await endpoint.Handle(context, CancellationToken.None);

        Assert.Equal(StatusCodes.Status502BadGateway,
            Assert.IsAssignableFrom<IStatusCodeHttpResult>(result).StatusCode);
        Assert.Equal("nosniff", context.Response.Headers["X-Content-Type-Options"].ToString());
        Assert.Equal(1, handler.CallCount);
    }

    private static IConfiguration CreateConfiguration() =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["GraphHopperRouteUrl"] = "https://example.test/route",
            })
            .Build();

    private static DefaultHttpContext CreateContext(string body)
    {
        var context = new DefaultHttpContext();
        context.Request.Method = HttpMethods.Post;
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
        return context;
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        public int CallCount { get; private set; }
        public string ResponseBody { get; set; } = "{\"ok\":true}";

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            CallCount++;
            return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(ResponseBody, Encoding.UTF8, "text/html"),
            };
        }
    }
}
