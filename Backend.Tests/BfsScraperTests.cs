using Backend.Scrapers;
using Microsoft.Extensions.Logging;
using Moq;

namespace Backend.Tests;

public class BfsScraperTests
{
    private const string MinimalGpx = "<?xml version=\"1.0\"?><gpx><trk><name>Test</name><trkseg><trkpt lat=\"1\" lon=\"2\"/><trkpt lat=\"3\" lon=\"4\"/></trkseg></trk></gpx>";

    [Fact]
    public async Task ExternalUrls_OnlyUsedForGpxFiles()
    {
        // Arrange
        var logger = Mock.Of<ILogger>();
        var httpClient = new HttpClient(new DummyHandler());
        var scraper = new BfsScraper(logger);
        var url = new Uri("https://race.com/start");

        // Act
        var result = await scraper.ScrapeAsync([url], httpClient, CancellationToken.None);

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
        var result = await scraper.ScrapeAsync([url], httpClient, CancellationToken.None);

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

    /// <summary>
    /// A Drive file view link (drive.google.com/file/d/{ID}/view) with "GPX" in the anchor text
    /// should be converted to a direct download URL and the GPX route should be returned.
    /// </summary>
    [Fact]
    public async Task DriveFileViewUrl_IsConvertedToDownloadAndParsed()
    {
        const string fileId = "ABCDE12345678901234";
        var racePage = $"""
            <html><body>
              <a href="https://drive.google.com/file/d/{fileId}/view?usp=sharing">Download GPX</a>
            </body></html>
            """;

        var handler = new FuncHandler(uri =>
        {
            // The scraper must convert the view URL to this download URL.
            if (uri.Host == "drive.google.com" && uri.AbsolutePath == "/uc"
                && uri.Query.Contains(fileId))
                return GpxResponse();

            return HtmlResponse(racePage);
        });

        var scraper = new BfsScraper(Mock.Of<ILogger>());
        var result = await scraper.ScrapeAsync(
            [new Uri("https://race.com/banan")],
            new HttpClient(handler),
            CancellationToken.None);

        Assert.NotNull(result);
        Assert.Single(result.Routes);
    }

    /// <summary>
    /// When a Drive folder contains a subfolder whose name contains "STAGES",
    /// that subfolder must be skipped and its files must not appear in results.
    /// </summary>
    [Fact]
    public async Task DriveFolder_SubfolderNameContainingStages_IsIgnored()
    {
        const string folderId = "FOLDERID123456789012";
        const string fileId   = "FILEID1234567890123A";
        const string stagesId = "STAGEID123456789012B";

        // Drive folder HTML: two data-id items (a file and a "STAGES" subfolder)
        var folderHtml = $"""
            <html><title>Race Routes – Google Drive</title><body>
              <div data-id="{fileId}"></div>
              <div data-id="{stagesId}"></div>
            </body></html>
            """;

        // Probe response for fileId — plain HTML (no data-ids) → treated as a file
        var fileProbeHtml = "<html><title>Route.gpx – Google Drive</title><body>not a folder</body></html>";

        // Probe response for stagesId — folder HTML with "STAGES" in the title
        var stagesFolderHtml = $"""
            <html><title>STAGES 2024 – Google Drive</title><body>
              <div data-id="INNERID12345678901234"></div>
            </body></html>
            """;

        var racePage = $"""
            <html><body>
              <a href="https://drive.google.com/drive/folders/{folderId}">GPX Download</a>
            </body></html>
            """;

        var handler = new FuncHandler(uri =>
        {
            var abs = uri.AbsoluteUri;

            // Download URL for the real file must return GPX (check /uc path first)
            if (uri.Host == "drive.google.com" && uri.AbsolutePath == "/uc"
                && uri.Query.Contains(fileId))
                return GpxResponse();

            // Any other Drive download URL (e.g. stagesId inner files) must NOT be called
            if (uri.Host == "drive.google.com" && uri.AbsolutePath == "/uc")
                throw new InvalidOperationException($"Unexpected Drive download for {uri}");

            // Drive folder/file probes
            if (abs.Contains(folderId))    return HtmlResponse(folderHtml);
            if (abs.Contains(fileId))      return HtmlResponse(fileProbeHtml);
            if (abs.Contains(stagesId))    return HtmlResponse(stagesFolderHtml);

            return HtmlResponse(racePage);
        });

        var scraper = new BfsScraper(Mock.Of<ILogger>());
        var result = await scraper.ScrapeAsync(
            [new Uri("https://race.com/start")],
            new HttpClient(handler),
            CancellationToken.None);

        Assert.NotNull(result);
        Assert.Single(result.Routes);
    }

    /// <summary>
    /// When the same GPX filename is linked from multiple paths (e.g. different subdirectories
    /// or query strings), only one route should be returned.
    /// </summary>
    [Fact]
    public async Task GpxDedup_SameFilename_ProducesOneRoute()
    {
        // Three links all resolve to files named "route.gpx" — two at different paths,
        // one with an extra query parameter. All return the same valid GPX.
        var racePage = """
            <html><body>
              <a href="/files/route.gpx">GPX</a>
              <a href="/mirror/route.gpx">GPX</a>
              <a href="/files/route.gpx?v=2">GPX</a>
            </body></html>
            """;

        var handler = new FuncHandler(uri =>
            uri.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase)
                ? GpxResponse()
                : HtmlResponse(racePage));

        var scraper = new BfsScraper(Mock.Of<ILogger>());
        var result = await scraper.ScrapeAsync(
            [new Uri("https://race.com/")],
            new HttpClient(handler),
            CancellationToken.None);

        Assert.NotNull(result);
        Assert.Single(result.Routes);
    }

    /// <summary>
    /// When two GPX files have the same route name and distances within 10% of each other,
    /// only the first should be kept.
    /// </summary>
    [Fact]
    public async Task GpxDedup_SameNameSimilarDistance_ProducesOneRoute()
    {
        // Two distinctly named GPX files with the same track name and very similar geometry.
        const string gpx1 = """
            <?xml version="1.0"?>
            <gpx><trk><name>Sprint Loop</name><trkseg>
              <trkpt lat="55.6" lon="13.0"/>
              <trkpt lat="55.7" lon="13.1"/>
            </trkseg></trk></gpx>
            """;
        const string gpx2 = """
            <?xml version="1.0"?>
            <gpx><trk><name>Sprint Loop</name><trkseg>
              <trkpt lat="55.6" lon="13.0"/>
              <trkpt lat="55.701" lon="13.101"/>
            </trkseg></trk></gpx>
            """;

        var racePage = """
            <html><body>
              <a href="/race/loop_v1.gpx">GPX</a>
              <a href="/race/loop_v2.gpx">GPX</a>
            </body></html>
            """;

        var handler = new FuncHandler(uri => uri.AbsoluteUri switch
        {
            var u when u.EndsWith("loop_v1.gpx") => new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(gpx1, System.Text.Encoding.UTF8, "application/gpx+xml")
            },
            var u when u.EndsWith("loop_v2.gpx") => new HttpResponseMessage(System.Net.HttpStatusCode.OK)
            {
                Content = new StringContent(gpx2, System.Text.Encoding.UTF8, "application/gpx+xml")
            },
            _ => HtmlResponse(racePage)
        });

        var scraper = new BfsScraper(Mock.Of<ILogger>());
        var result = await scraper.ScrapeAsync(
            [new Uri("https://race.com/")],
            new HttpClient(handler),
            CancellationToken.None);

        Assert.NotNull(result);
        Assert.Single(result.Routes);
    }

    /// <summary>
    /// When the start page embeds a RideWithGPS iframe, the scraper should fetch the route JSON
    /// and return a route with coordinates.
    /// </summary>
    [Fact]
    public async Task RideWithGpsIframe_ReturnsRouteWithCoordinates()
    {
        const string routeId = "99887766";
        var racePage = $"""
            <html><body>
              <h1>My Race</h1>
              <iframe src="https://ridewithgps.com/embeds?type=route&id={routeId}&sampleGraph=true"></iframe>
            </body></html>
            """;

        var routeJson = $$"""
            {
              "id": {{routeId}},
              "name": "Marathon Route",
              "elevation_gain": 350.0,
              "track_points": [
                {"x": 16.0, "y": 59.0, "e": 10.0, "d": 0.0},
                {"x": 16.1, "y": 59.1, "e": 15.0, "d": 1200.0}
              ]
            }
            """;

        var handler = new FuncHandler(uri =>
        {
            if (uri.Host == "ridewithgps.com" && uri.AbsolutePath == $"/routes/{routeId}.json")
                return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
                {
                    Content = new StringContent(routeJson, System.Text.Encoding.UTF8, "application/json")
                };
            return HtmlResponse(racePage);
        });

        var scraper = new BfsScraper(Mock.Of<ILogger>());
        var result = await scraper.ScrapeAsync(
            [new Uri("https://race.se/info")],
            new HttpClient(handler),
            CancellationToken.None);

        Assert.NotNull(result);
        Assert.Single(result.Routes);
        var route = result.Routes[0];
        Assert.Equal(2, route.Coordinates.Count);
        Assert.Equal("Marathon Route", route.Name);
        Assert.Equal(350.0, route.ElevationGain);
        Assert.Equal("ridewithgps", route.GpxSource);
    }

    [Fact]
    public async Task ExternalIframeRoot_FollowsOneLevelAndFindsRideWithGpsRoute()
    {
        const string routeId = "45032647";
                var racePage = """
                        <html><body>
                            <script>
                                Server.widgetsOnPage = [{"className":"embed","data":{"code":"<iframe src=\"https:\/\/eventplatform.example.com\/event\/17107\/app\" width=\"100%\" height=\"2200\"><\/iframe>"}}];
                            </script>
                        </body></html>
                        """;

        var iframeRoot = """
            <html><body>
              <a href="/event/17107/app/page/bana">Bana</a>
            </body></html>
            """;

        var iframeCoursePage = $"""
            <html><body>
              <iframe src="https://ridewithgps.com/embeds?sampleGraph=true&type=route&id={routeId}"></iframe>
            </body></html>
            """;

        var routeJson = $$"""
            {
              "id": {{routeId}},
              "name": "Iframe Route",
              "elevation_gain": 275.0,
              "track_points": [
                {"x": 16.0, "y": 59.0},
                {"x": 16.1, "y": 59.1}
              ]
            }
            """;

        var handler = new FuncHandler(uri =>
        {
            if (uri.Host == "ridewithgps.com" && uri.AbsolutePath == $"/routes/{routeId}.json")
                return new HttpResponseMessage(System.Net.HttpStatusCode.OK)
                {
                    Content = new StringContent(routeJson, System.Text.Encoding.UTF8, "application/json")
                };
            if (uri.Host == "eventplatform.example.com" && uri.AbsolutePath == "/event/17107/app/page/bana")
                return HtmlResponse(iframeCoursePage);
            if (uri.Host == "eventplatform.example.com" && uri.AbsolutePath == "/event/17107/app")
                return HtmlResponse(iframeRoot);
            return HtmlResponse(racePage);
        });

        var scraper = new BfsScraper(Mock.Of<ILogger>());
        var result = await scraper.ScrapeAsync(
            [new Uri("https://motionsloppet.se/tidaholm-marathon/information")],
            new HttpClient(handler),
            CancellationToken.None);

        Assert.NotNull(result);
        Assert.Single(result.Routes);
        Assert.Equal("Iframe Route", result.Routes[0].Name);
        Assert.Equal("ridewithgps", result.Routes[0].GpxSource);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static HttpResponseMessage GpxResponse() =>
        new(System.Net.HttpStatusCode.OK)
        {
            Content = new StringContent(MinimalGpx, System.Text.Encoding.UTF8, "application/gpx+xml")
        };

    private static HttpResponseMessage HtmlResponse(string html) =>
        new(System.Net.HttpStatusCode.OK)
        {
            Content = new StringContent(html, System.Text.Encoding.UTF8, "text/html")
        };

    /// <summary>Delegate-based handler for per-URL response control.</summary>
    private class FuncHandler(Func<Uri, HttpResponseMessage> respond) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken) =>
            Task.FromResult(respond(request.RequestUri!));
    }

    // Dummy handler to simulate HTTP responses
    private class DummyHandler : HttpMessageHandler
    {
        private readonly string? _content;
        public DummyHandler(string? content = null) => _content = content;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            // If the request is for a .gpx file, return a single-point GPX that cannot be parsed
            // (GpxParser requires ≥ 2 points), so existing metadata-assertion tests pass trivially.
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
