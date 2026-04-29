using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;

namespace Backend;

/// <summary>
/// Timer trigger (Tuesday 03:00 UTC): enqueues one non-urgent scrape job per stale organizer.
/// When invoked manually via the Azure Functions admin endpoint
/// (<c>POST /admin/functions/QueueAllScrapeJobs</c>), <see cref="TimerInfo.ScheduleStatus"/>
/// is null — treat that as an explicit request to re-scrape ALL organizers urgently.
/// </summary>
public class QueueAllScrapeJobs(
    BlobOrganizerStore raceOrganizerClient,
    RaceDiscoveryService discoveryService,
    ILogger<QueueAllScrapeJobs> logger)
{
    [Function(nameof(QueueAllScrapeJobs))]
    public async Task Run([TimerTrigger("0 0 3 * * 2")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        // ScheduleStatus is null when triggered manually via the admin endpoint.
        bool isManual = timerInfo.ScheduleStatus is null;

        if (isManual)
        {
            // Queue every known organizer as an urgent scrape.
            var allIds = await raceOrganizerClient.GetIdsDueForAutomaticScrapeAsync(DateTime.MaxValue, cancellationToken);
            logger.LogInformation("Manual trigger: queuing {Count} urgent scrape jobs for all organizers", allIds.Count);
            await discoveryService.EnqueueScrapeMessagesAsync(allIds.ToHashSet(StringComparer.OrdinalIgnoreCase), cancellationToken, isUrgent: true);
        }
        else
        {
            // Scheduled run: only queue organizers not scraped recently.
            var cutoffUtc = DateTime.UtcNow.Subtract(RaceDiscoveryService.AutomaticScrapeFreshnessWindow);
            var ids = await raceOrganizerClient.GetIdsDueForAutomaticScrapeAsync(cutoffUtc, cancellationToken);
            logger.LogInformation("Scheduled trigger: queuing {Count} automatic scrape jobs for organizers not scraped since {CutoffUtc}", ids.Count, cutoffUtc);
            await discoveryService.EnqueueScrapeMessagesAsync(ids.ToHashSet(StringComparer.OrdinalIgnoreCase), cancellationToken, isUrgent: false);
        }
    }
}
