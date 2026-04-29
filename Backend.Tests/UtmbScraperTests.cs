using System.Net;
using System.Text;
using Backend.Scrapers;
using Microsoft.Extensions.Logging;

namespace Backend.Tests;

public class UtmbScraperTests
{
    [Fact]
    public async Task ScrapeAsync_ReturnsEmptyResultWhenPageHasNoGpxLinks()
    {
        var logger = new LoggerFactory().CreateLogger<UtmbScraper>();
        var scraper = new UtmbScraper(logger);
        var job = new ScrapeJob(UtmbUrl: new Uri("https://utmb.world/races/example"));
        using var client = new HttpClient(new StubHttpMessageHandler("<html><body>No GPX here</body></html>", "text/html"));

        var result = await scraper.ScrapeAsync(job, client, CancellationToken.None);

        Assert.NotNull(result);
        Assert.Empty(result.Routes);
    }

    private sealed class StubHttpMessageHandler(string content, string mediaType) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
            => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(content, Encoding.UTF8, mediaType)
            });
    }
}