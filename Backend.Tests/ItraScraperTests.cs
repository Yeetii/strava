using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
using Backend.Scrapers;

namespace Backend.Tests;

public class ItraScraperTests
{
    [Fact]
    public void CanHandle_ReturnsTrueForGetTraceItraUrl()
    {
        var logger = new LoggerFactory().CreateLogger<ItraScraper>();
        var scraper = new ItraScraper(logger);
        var job = new ScrapeJob(
            TraceDeTrailItraUrls: [new Uri("https://tracedetrail.fr/trace/getTraceItra/12345")]);

        Assert.True(scraper.CanHandle(job));
    }

    [Fact]
    public void CanHandle_ReturnsFalseForOtherTraceUrls()
    {
        var logger = new LoggerFactory().CreateLogger<ItraScraper>();
        var scraper = new ItraScraper(logger);
        var job = new ScrapeJob(
            TraceDeTrailItraUrls: [new Uri("https://tracedetrail.fr/en/trace/12345")]);

        Assert.False(scraper.CanHandle(job));
    }

    [Fact]
    public void BuildScrapeJobFromDocument_PicksUpOnlyTraceGetTraceItraUrls()
    {
        var doc = new Shared.Models.RaceOrganizerDocument
        {
            Id = "example.com",
            Url = "https://example.com/",
            Discovery = new Dictionary<string, List<Shared.Models.SourceDiscovery>>
            {
                ["itrabackend"] = new List<Shared.Models.SourceDiscovery>
                {
                    new()
                    {
                        DiscoveredAtUtc = DateTime.UtcNow.ToString("o"),
                        SourceUrls = new List<string>
                        {
                            "https://itra.run/Races/RaceDetails/1234",
                            "https://tracedetrail.fr/trace/getTraceItra/12345",
                        }
                    }
                }
            }
        };

        var job = ScrapeRaceWorker.BuildScrapeJobFromDocument(doc);

        Assert.Null(job.UtmbUrl);
        Assert.Null(job.RunagainUrl);
        Assert.Single(job.TraceDeTrailItraUrls!);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/12345", job.TraceDeTrailItraUrls![0].AbsoluteUri);
        Assert.Null(job.TraceDeTrailEventUrl);
    }

    [Fact]
    public async Task ScrapeAsync_ReturnsEmptyResultWhenNoRoutesAreFound()
    {
        var logger = new LoggerFactory().CreateLogger<ItraScraper>();
        var scraper = new ItraScraper(logger);
        var job = new ScrapeJob(
            TraceDeTrailItraUrls: [new Uri("https://tracedetrail.fr/trace/getTraceItra/12345")]);
        using var client = new HttpClient(new StubHttpMessageHandler("""
            {
              "points": []
            }
            """));

        var result = await scraper.ScrapeAsync(job, client, CancellationToken.None);

        Assert.NotNull(result);
        Assert.Empty(result.Routes);
    }

    private sealed class StubHttpMessageHandler(string content) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content, Encoding.UTF8, "application/json")
            });
    }
}
