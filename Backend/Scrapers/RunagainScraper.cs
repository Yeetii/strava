using Microsoft.Extensions.Logging;

namespace Backend.Scrapers;

// Placeholder scraper for the Runagain source.
// TODO: implement discovery in QueueScrapeRaceJobs.FetchRunagainJobsAsync and scraping here
//       once the Runagain API endpoint and response format are known.
internal sealed class RunagainScraper(ILogger logger) : IRaceScraper
{
    public bool CanHandle(ScrapeJob job) => job.RunagainUrl is not null;

    public Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken)
    {
        if (job.RunagainUrl is null) return Task.FromResult<RaceScraperResult?>(null);

        logger.LogDebug("RunagainScraper: scraping {Url} (not yet implemented)", job.RunagainUrl);
        return Task.FromResult<RaceScraperResult?>(null);
    }
}
