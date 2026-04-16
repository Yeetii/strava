using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend.Scrapers;

// Fetches the UTMB race HTML page, extracts .gpx hrefs, downloads each file, and returns the parsed routes.
internal sealed class UtmbScraper(ILogger logger) : IRaceScraper
{
    public bool CanHandle(ScrapeJob job) => job.UtmbUrl is not null;

    public async Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.UtmbUrl is null) return null;

        string html;
        try
        {
            html = await httpClient.GetStringAsync(job.UtmbUrl, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "UTMB: failed to fetch race page {Url}", job.UtmbUrl);
            return null;
        }

        var gpxUrls = RaceHtmlScraper.ExtractGpxUrlsFromHtml(html, job.UtmbUrl)
            .GroupBy(u => u.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList();

        if (gpxUrls.Count == 0)
        {
            logger.LogWarning("UTMB: no GPX links found on race page {Url}", job.UtmbUrl);
            return null;
        }

        var routes = new List<ScrapedRoute>();
        foreach (var gpxUrl in gpxUrls)
        {
            var route = await TryFetchGpxRouteAsync(httpClient, gpxUrl, job, cancellationToken);
            if (route is not null)
                routes.Add(route);
        }

        return routes.Count > 0 ? new RaceScraperResult(routes) : null;
    }

    private async Task<ScrapedRoute?> TryFetchGpxRouteAsync(
        HttpClient httpClient,
        Uri gpxUrl,
        ScrapeJob job,
        CancellationToken cancellationToken)
    {
        try
        {
            var gpxContent = await httpClient.GetStringAsync(gpxUrl, cancellationToken);
            var parsed = GpxParser.TryParseRoute(gpxContent, job.Name ?? "Unnamed route");
            if (parsed is null)
            {
                logger.LogWarning("UTMB: skipping GPX {GpxUrl}: failed to parse route points", gpxUrl);
                return null;
            }

            return new ScrapedRoute(
                Coordinates: parsed.Coordinates,
                SourceUrl: job.UtmbUrl,
                Name: parsed.Name,
                GpxUrl: gpxUrl);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "UTMB: failed to fetch or parse GPX from {GpxUrl}", gpxUrl);
            return null;
        }
    }
}
