using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Stigar.Core.Services;
using System.Net;
using System.Text.Json;
using Stigar.Core.Services.Scrapers;
using Microsoft.Extensions.Logging;

namespace Stigar.Api.Endpoints.Backend;

public class TestScrapeUrl(
    IHttpClientFactory httpClientFactory,
    ILogger<TestScrapeUrl> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/scrape/test", (HttpContext context, [FromServices] TestScrapeUrl endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        var url = context.Request.Query["url"].ToString();
        if (string.IsNullOrWhiteSpace(url) || !Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            return TypedResults.BadRequest("Missing or invalid 'url' query parameter");
        }

        logger.LogInformation("TestScrapeUrl: BFS-scraping {Url}", url);

        var scraper = new BfsScraper(logger);
        var httpClient = httpClientFactory.CreateClient();
        var result = await scraper.ScrapeAsync([uri], httpClient, cancellationToken);

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
                gpxSource = r.GpxSource,
            }) ?? [],
        };

        var json = JsonSerializer.Serialize(body, new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        });
        return TypedResults.Text(json, "application/json; charset=utf-8");
    }
}
