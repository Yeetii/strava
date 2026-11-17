using System.Net;
using Backend.Scrapers;
using Microsoft.Extensions.Logging;

namespace Backend.Tests;

public class PlotARouteScraperTests
{
    private static readonly Uri SourcePage = new("https://example.com/race");

    [Fact]
    public void ExtractRouteIds_SkipsZeroAndKeepsValidIds()
    {
        const string html = """
            <a href="/route/0">placeholder</a>
            <a href="https://www.plotaroute.com/route/12345">one</a>
            <a href="https://plotaroute.com/route/67890?units=km">two</a>
            """;

        var ids = PlotARouteScraper.ExtractRouteIds(html);

        Assert.Equal(["12345", "67890"], ids);
    }

    [Fact]
    public void TryGetRouteId_AcceptsValidId()
    {
        Assert.True(PlotARouteScraper.TryGetRouteId(new Uri("https://www.plotaroute.com/route/12345"), out var routeId));
        Assert.Equal("12345", routeId);
    }

    [Fact]
    public void TryGetRouteId_RejectsZeroId()
    {
        Assert.False(PlotARouteScraper.TryGetRouteId(new Uri("https://www.plotaroute.com/route/0"), out _));
    }

    [Fact]
    public async Task ScrapeRouteAsync_ZeroId_MakesNoRequest()
    {
        var handler = new DelegateHandler(_ => throw new InvalidOperationException("No request expected"));
        using var client = new HttpClient(handler);
        var logger = new CapturingLogger();
        var scraper = new PlotARouteScraper(logger);

        var route = await scraper.ScrapeRouteAsync("0", SourcePage, client, CancellationToken.None);

        Assert.Null(route);
        Assert.Equal(0, handler.CallCount);
    }

    [Fact]
    public async Task ScrapeRouteAsync_CloudflareChallenge_LogsOnceAtDebugAndNeverWarns()
    {
        var handler = new DelegateHandler(_ => new HttpResponseMessage(HttpStatusCode.Forbidden)
        {
            Headers = { { "cf-mitigated", "challenge" } },
            Content = new StringContent("Just a moment...")
        });
        using var client = new HttpClient(handler);
        var logger = new CapturingLogger();
        var scraper = new PlotARouteScraper(logger);

        Assert.Null(await scraper.ScrapeRouteAsync("23392798", SourcePage, client, CancellationToken.None));
        Assert.Null(await scraper.ScrapeRouteAsync("14525428", SourcePage, client, CancellationToken.None));

        Assert.Equal(2, handler.CallCount);
        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Warning);
        var challengeLogs = logger.Entries.Where(e => e.Level == LogLevel.Debug && e.Message.Contains("Cloudflare")).ToList();
        Assert.Single(challengeLogs);
    }

    [Fact]
    public async Task ScrapeRouteAsync_NonChallengeHttpError_StillWarns()
    {
        var handler = new DelegateHandler(_ => new HttpResponseMessage(HttpStatusCode.InternalServerError));
        using var client = new HttpClient(handler);
        var logger = new CapturingLogger();
        var scraper = new PlotARouteScraper(logger);

        var route = await scraper.ScrapeRouteAsync("12345", SourcePage, client, CancellationToken.None);

        Assert.Null(route);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task ScrapeRouteAsync_ValidJson_ReturnsRoute()
    {
        const string json = """
            {
              "RouteName": "Test Route",
              "Distance": 42000,
              "Ascent": 350,
              "RouteData": "[{\"lat\":59.3,\"lng\":18.1},{\"lat\":59.4,\"lng\":18.2},{\"lat\":59.5,\"lng\":18.3}]"
            }
            """;
        var handler = new DelegateHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(json)
        });
        using var client = new HttpClient(handler);
        var logger = new CapturingLogger();
        var scraper = new PlotARouteScraper(logger);

        var route = await scraper.ScrapeRouteAsync("12345", SourcePage, client, CancellationToken.None);

        Assert.NotNull(route);
        Assert.Equal("Test Route", route.Name);
        Assert.Equal(3, route.Coordinates.Count);
        Assert.Equal(SourcePage, route.SourceUrl);
    }

    private sealed class DelegateHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        public int CallCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            CallCount++;
            return Task.FromResult(respond(request));
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Entries.Add((logLevel, formatter(state, exception)));

        private sealed class NullScope : IDisposable
        {
            public static readonly NullScope Instance = new();
            public void Dispose() { }
        }
    }
}
