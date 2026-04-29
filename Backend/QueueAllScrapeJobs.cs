using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend;

/// <summary>
/// Runs the day after all discovery timer triggers (Tuesday 03:00 UTC) and enqueues one
/// scrape job per known race organizer. Detaches scraping from discovery so that a race
/// discovered by multiple sources (e.g. UTMB + ITRA + DUV) only produces a single scrape job.
/// </summary>
public class QueueAllScrapeJobs(
    BlobOrganizerStore raceOrganizerClient,
    RaceDiscoveryService discoveryService,
    ILogger<QueueAllScrapeJobs> logger)
{
    [Function(nameof(QueueAllScrapeJobs))]
    public async Task Run([TimerTrigger("0 0 3 * * 2")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var cutoffUtc = DateTime.UtcNow.Subtract(RaceDiscoveryService.AutomaticScrapeFreshnessWindow);
        var ids = await raceOrganizerClient.GetIdsDueForAutomaticScrapeAsync(cutoffUtc, cancellationToken);
        logger.LogInformation("Queuing {Count} automatic scrape jobs for organizers not scraped since {CutoffUtc}", ids.Count, cutoffUtc);
        await discoveryService.EnqueueScrapeMessagesAsync(ids.ToHashSet(StringComparer.OrdinalIgnoreCase), cancellationToken, isUrgent: false);
    }
}
