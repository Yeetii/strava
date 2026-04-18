using System.Net;
using System.Text.Json;
using Backend.Scrapers;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;

namespace Backend;

public class TestScrapeUrl(
    IHttpClientFactory httpClientFactory,
    ILogger<TestScrapeUrl> logger)
{
    [Function(nameof(TestScrapeUrl))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "scrape/test")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var url = req.Query["url"];
        if (string.IsNullOrWhiteSpace(url) || !Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            await badRequest.WriteStringAsync("Missing or invalid 'url' query parameter");
            return badRequest;
        }

        logger.LogInformation("TestScrapeUrl: BFS-scraping {Url}", url);

        var scraper = new BfsScraper(logger);
        var httpClient = httpClientFactory.CreateClient();
        var result = await scraper.ScrapeAsync([uri], eventName: null, distance: null, httpClient, cancellationToken);

        var body = new
        {
            hasRoutes = result is { Routes.Count: > 0 },
            routeCount = result?.Routes.Count ?? 0,
            websiteUrl = result?.WebsiteUrl?.AbsoluteUri,
            imageUrl = result?.ImageUrl?.AbsoluteUri,
            logoUrl = result?.LogoUrl?.AbsoluteUri,
            extractedName = result?.ExtractedName,
            extractedDate = result?.ExtractedDate,
            startFee = result?.StartFee,
            currency = result?.Currency,
            routes = result?.Routes.Select(r => new
            {
                name = r.Name,
                distance = r.Distance,
                elevationGain = r.ElevationGain,
                gpxUrl = r.GpxUrl?.AbsoluteUri,
                sourceUrl = r.SourceUrl?.AbsoluteUri,
                coordinateCount = r.Coordinates.Count,
                imageUrl = r.ImageUrl?.AbsoluteUri,
                logoUrl = r.LogoUrl?.AbsoluteUri,
                date = r.Date,
                startFee = r.StartFee,
                currency = r.Currency,
            }) ?? [],
        };

        var response = req.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/json");
        await response.WriteStringAsync(JsonSerializer.Serialize(body, new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        }));
        return response;
    }
}
