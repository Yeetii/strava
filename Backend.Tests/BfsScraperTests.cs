using Backend.Scrapers;
using Microsoft.Extensions.Logging;
using Moq;

namespace Backend.Tests;

public class BfsScraperTests
{

    [Fact]
    public async Task ExternalUrls_OnlyUsedForGpxFiles()
    {
        // Arrange
        var logger = Mock.Of<ILogger>();
        var httpClient = new HttpClient(new DummyHandler());
        var scraper = new BfsScraper(logger);
        var url = new Uri("https://race.com/start");

        // Act
        var result = await scraper.ScrapeAsync([url], null, null, httpClient, CancellationToken.None);

        // Assert
        // Should not extract metadata from external pages, only GPX links
        Assert.All(result?.Routes ?? [], route =>
        {
            Assert.True(route.SourceUrl == url, "Route data must come from the race page");
            Assert.Null(route.Name);
            Assert.Null(route.Distance);
            Assert.Null(route.ElevationGain);
            Assert.Null(route.ImageUrl);
            Assert.Null(route.LogoUrl);
            Assert.Null(route.Date);
            Assert.Null(route.StartFee);
            Assert.Null(route.Currency);
            Assert.Null(route.RaceType);
        });
    }



    [Fact]
    public async Task OnlyRacePages_UsedForMetadata()
    {
        // Arrange
        var logger = Mock.Of<ILogger>();
        var httpClient = new HttpClient(new DummyHandler());
        var scraper = new BfsScraper(logger);
        var url = new Uri("https://race.com/page");

        // Act
        var result = await scraper.ScrapeAsync([url], null, null, httpClient, CancellationToken.None);

        // Assert
        Assert.All(result?.Routes ?? [], route =>
        {
            Assert.True(route.SourceUrl == url, "Metadata must be extracted only from race pages");
            // All metadata fields should be null except SourceUrl and Coordinates
            Assert.Null(route.Name);
            Assert.Null(route.Distance);
            Assert.Null(route.ElevationGain);
            Assert.Null(route.ImageUrl);
            Assert.Null(route.LogoUrl);
            Assert.Null(route.Date);
            Assert.Null(route.StartFee);
            Assert.Null(route.Currency);
            Assert.Null(route.RaceType);
        });
    }

    // Dummy handler to simulate HTTP responses
    private class DummyHandler : HttpMessageHandler
    {
        private readonly string? _content;
        public DummyHandler(string? content = null) => _content = content;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // If the request is for a .gpx file, return a minimal valid GPX
            if (request.RequestUri != null && request.RequestUri.AbsoluteUri.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
            {
                var gpx = "<?xml version=\"1.0\"?><gpx><trk><name>Test</name><trkseg><trkpt lat=\"1\" lon=\"2\"/></trkseg></trk></gpx>";
                return Task.FromResult(new HttpResponseMessage(System.Net.HttpStatusCode.OK)
                {
                    Content = new StringContent(gpx, System.Text.Encoding.UTF8, "application/gpx+xml")
                });
            }
            var html = _content ?? "<html><body>race page<a href=\"test.gpx\">Download GPX</a></body></html>";
            var response = new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(html)
            };
            return Task.FromResult(response);
        }
    }
}
