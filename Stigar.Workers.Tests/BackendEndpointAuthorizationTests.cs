using System.Net;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Stigar.Api.Endpoints.Backend;
using Stigar.Api.Http;
using Stigar.Core.Services;

namespace Stigar.Workers.Tests;

/// <summary>
/// Regression tests for admin authorization on the backend batch endpoints.
/// Unauthenticated calls must be rejected with 401 before any work is done;
/// calls carrying the configured admin key must pass the guard.
/// </summary>
public class BackendEndpointAuthorizationTests : IAsyncLifetime
{
    private const string AdminKey = "test-admin-key";
    private WebApplication _app = null!;
    private HttpClient _client = null!;

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddSingleton<IConfiguration>(CreateConfiguration());
        builder.Services.AddLogging();
        builder.Services.AddHttpClient();

        var organizerStore = new Mock<IOrganizerStore>();
        organizerStore
            .Setup(store => store.GetIdsDueForAutomaticScrapeAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<string>());
        builder.Services.AddSingleton(organizerStore.Object);
        builder.Services.AddSingleton(new Mock<IBackgroundJobClient>().Object);
        builder.Services.AddSingleton<RaceDiscoveryService>();
        builder.Services.AddTransient<QueueAllScrapeJobsHttp>();

        _app = builder.Build();
        QueueAllScrapeJobsHttp.Map(_app);
        _app.Urls.Add("http://127.0.0.1:0");
        await _app.StartAsync();

        var addresses = _app.Services.GetRequiredService<IServer>()
            .Features.Get<IServerAddressesFeature>()!.Addresses;
        _client = new HttpClient { BaseAddress = new Uri(addresses.Single()) };
    }

    public async Task DisposeAsync()
    {
        _client.Dispose();
        await _app.DisposeAsync();
    }

    [Fact]
    public async Task QueueAllScrapeJobsHttp_WithoutAdminKey_ReturnsUnauthorized()
    {
        var response = await _client.PostAsync("/api/manage/queue-all-scrape-jobs", content: null);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task QueueAllScrapeJobsHttp_WithWrongAdminKey_ReturnsUnauthorized()
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/manage/queue-all-scrape-jobs");
        request.Headers.Add("x-admin-key", "wrong-key");

        var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task QueueAllScrapeJobsHttp_WithValidAdminKey_IsAuthorized()
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, "/api/manage/queue-all-scrape-jobs");
        request.Headers.Add("x-admin-key", AdminKey);

        var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public void IsAdminAuthorized_NoConfiguredKey_ReturnsFalse()
    {
        var context = CreateContext();
        var configuration = new ConfigurationBuilder().Build();

        Assert.False(context.IsAdminAuthorized(configuration));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("wrong-key")]
    public void IsAdminAuthorized_MissingOrWrongHeader_ReturnsFalse(string? providedKey)
    {
        var context = CreateContext();
        if (providedKey is not null)
            context.Request.Headers["x-admin-key"] = providedKey;

        Assert.False(context.IsAdminAuthorized(CreateConfiguration()));
    }

    [Fact]
    public void IsAdminAuthorized_ValidHeader_ReturnsTrue()
    {
        var context = CreateContext();
        context.Request.Headers["x-admin-key"] = AdminKey;

        Assert.True(context.IsAdminAuthorized(CreateConfiguration()));
    }

    [Theory]
    [MemberData(nameof(ProtectedEndpoints))]
    public async Task UnauthenticatedCall_ReturnsUnauthorized(
        string endpointName,
        Func<HttpContext, Task<IResult>> handle)
    {
        var result = await handle(CreateContext());

        Assert.True(result is UnauthorizedHttpResult, $"{endpointName} did not return 401");
    }

    public static IEnumerable<object[]> ProtectedEndpoints()
    {
        var config = CreateConfiguration();
        yield return Endpoint("QueueAllScrapeJobsHttp", ctx => new QueueAllScrapeJobsHttp(null!, null!, null!, config).Handle(ctx, default));
        yield return Endpoint("PostScrapeOrganizer", ctx => new PostScrapeOrganizer(null!, null!, config).Handle(ctx, default));
        yield return Endpoint("TestScrapeUrl", ctx => new TestScrapeUrl(null!, null!, config).Handle(ctx, default));
        yield return Endpoint("RunSummitsForActivity", ctx => new RunSummitsForActivity(null!, null!, null!, null!, null!, null!, null!, config).Handle(ctx, "activity:1", default));
        yield return Endpoint("BackfillVisitedPathOsmHighwayIds", ctx => new BackfillVisitedPathOsmHighwayIds(null!, null!, null!, config).Handle(ctx, default));
        yield return Endpoint("BackfillVisitedPathOsmHighwayIds/all", ctx => new BackfillVisitedPathOsmHighwayIds(null!, null!, null!, config).HandleAll(ctx, default));
        yield return Endpoint("BackfillAdminBoundaryMetrics", ctx => new BackfillAdminBoundaryMetrics(null!, null!, null!, null!, null!, config).Handle(ctx, default));
        yield return Endpoint("CleanupExcludedActivityTypes", ctx => new CleanupExcludedActivityTypes(null!, null!, config).Handle(ctx, default));
        yield return Endpoint("StoreCuratedPeaksGroups", ctx => new StoreCuratedPeaksGroups(null!, null!, config).Handle(ctx));
    }

    private static object[] Endpoint(string name, Func<HttpContext, Task<IResult>> handle) => [name, handle];

    private static IConfiguration CreateConfiguration() =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["AdminApiKey"] = AdminKey,
            })
            .Build();

    private static HttpContext CreateContext()
    {
        var context = new DefaultHttpContext();
        context.Request.Method = HttpMethods.Post;
        return context;
    }
}
