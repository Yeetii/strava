using Microsoft.Extensions.Logging;

namespace Backend.Scrapers;

// Fetches the TraceDeTrail event page, extracts the "Site de la course" anchor,
// and runs a BFS scrape on that race website URL.
internal sealed class TraceDeTrailEventScraper(ILogger logger, BfsScraper bfsScraper) : IRaceScraper
{
    public bool CanHandle(ScrapeJob job) => job.TraceDeTrailEventUrl is not null;

    public async Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.TraceDeTrailEventUrl is null) return null;

        string html;
        try
        {
            html = await httpClient.GetStringAsync(job.TraceDeTrailEventUrl, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "TraceDeTrail event page: failed to fetch {Url}", job.TraceDeTrailEventUrl);
            return null;
        }

        var siteUrl = RaceHtmlScraper.ExtractRaceSiteUrl(html, job.TraceDeTrailEventUrl);
        if (siteUrl is null)
        {
            logger.LogDebug("TraceDeTrail event page: no 'Site de la course' found on {Url}", job.TraceDeTrailEventUrl);
            return null;
        }

        logger.LogDebug("TraceDeTrail event page: found site URL {SiteUrl}, running BFS", siteUrl);
        var bfsResult = await bfsScraper.ScrapeFromUrlAsync(job, siteUrl, httpClient, cancellationToken);

        // Always return the website URL, even if BFS found no routes.
        var routes = bfsResult?.Routes ?? [];
        return new RaceScraperResult(routes, siteUrl);
    }
}
