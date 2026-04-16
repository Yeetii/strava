namespace Backend.Scrapers;

// Abstraction for a single race-data source.
// Each implementation handles one kind of URL (or set of URLs) in ScrapeJob and returns
// the routes it finds.  The worker tries implementations in priority order and upserts
// the first non-empty result.
public interface IRaceScraper
{
    // Returns true when this scraper can handle at least one URL in the job.
    bool CanHandle(ScrapeJob job);

    // Performs the scrape.  Returns null or an empty result when nothing was found.
    Task<RaceScraperResult?> ScrapeAsync(ScrapeJob job, HttpClient httpClient, CancellationToken cancellationToken);
}
