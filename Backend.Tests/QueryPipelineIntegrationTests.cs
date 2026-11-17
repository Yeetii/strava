using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Hosting;

namespace Backend.Tests;

/// <summary>
/// Drives a real HTTP request through FunctionHttpMapper + the Functions shim,
/// so a regression in query parsing is caught end-to-end rather than only at
/// the HttpRequestData collection level.
/// </summary>
public class QueryPipelineIntegrationTests
{
    [Fact]
    public async Task Get_WithoutOptionalQueryParams_ReturnsOkWithDefaults()
    {
        await using var fixture = await PipelineFixture.StartAsync();

        var response = await fixture.Client.GetAsync("/api/query-probe");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("6|2|False", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Get_WithOptionalQueryParams_ReturnsParsedValues()
    {
        await using var fixture = await PipelineFixture.StartAsync();

        var response = await fixture.Client.GetAsync(
            "/api/query-probe?zoom=12&adminLevel=4&forceRefresh=true");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("12|4|True", await response.Content.ReadAsStringAsync());
    }

    private sealed class PipelineFixture : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private PipelineFixture(WebApplication app, HttpClient client)
        {
            _app = app;
            Client = client;
        }

        public HttpClient Client { get; }

        public static async Task<PipelineFixture> StartAsync()
        {
            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            var app = builder.Build();
            app.MapFunctions(typeof(QueryProbeFunction).Assembly);
            await app.StartAsync();

            var address = app.Urls.First();
            return new PipelineFixture(app, new HttpClient { BaseAddress = new Uri(address) });
        }

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }
}

/// <summary>
/// Minimal trigger that reads optional query params exactly the way the real
/// endpoints do. Only this assembly is mapped, so no other Functions are hosted.
/// </summary>
public sealed class QueryProbeFunction
{
    [Function(nameof(QueryProbeFunction))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "query-probe")] HttpRequestData req)
    {
        var zoom = int.TryParse(req.Query["zoom"], out var z) ? z : 6;
        var adminLevel = int.TryParse(req.Query["adminLevel"], out var a) ? a : 2;
        var forceRefresh = string.Equals(req.Query["forceRefresh"], "true", StringComparison.OrdinalIgnoreCase);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteStringAsync($"{zoom}|{adminLevel}|{forceRefresh}");
        return response;
    }
}
