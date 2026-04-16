using Microsoft.Extensions.Logging;

namespace Backend.Scrapers;

// Fetches a RunAgain event page, looks for a direct GPX link first, then
// falls back to extracting the external race website and running BFS on it.
internal sealed class RunagainScraper(ILogger logger, BfsScraper bfsScraper) : IRaceScraper
{
    public bool CanHandle(ScrapeJob job) => job.RunagainUrl is not null;

    public async Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.RunagainUrl is null) return null;

        string html;
        try
        {
            html = await httpClient.GetStringAsync(job.RunagainUrl, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "RunAgain: failed to fetch event page {Url}", job.RunagainUrl);
            return null;
        }

        // 1. Try GPX links directly on the event page.
        var directGpxLinks = RaceHtmlScraper.ExtractGpxLinksFromHtml(html, job.RunagainUrl);
        if (directGpxLinks.Count > 0)
        {
            logger.LogDebug("RunAgain: found {Count} direct GPX links on {Url}", directGpxLinks.Count, job.RunagainUrl);
            // Delegate to BFS from the RunAgain event page itself — it will download and parse the GPX files.
            return await bfsScraper.ScrapeFromUrlAsync(job, job.RunagainUrl, httpClient, cancellationToken);
        }

        // 2. Try to find an external race website and BFS it.
        var siteUrl = RaceHtmlScraper.ExtractRunagainSiteUrl(html, job.RunagainUrl);
        if (siteUrl is not null)
        {
            logger.LogDebug("RunAgain: found external site {SiteUrl} on event page {EventUrl}, running BFS", siteUrl, job.RunagainUrl);
            return await bfsScraper.ScrapeFromUrlAsync(job, siteUrl, httpClient, cancellationToken);
        }

        logger.LogDebug("RunAgain: no GPX or external site found on event page {Url}", job.RunagainUrl);
        return null;
    }
}
