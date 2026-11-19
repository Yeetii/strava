using Stigar.Core.Jobs;
using Microsoft.Extensions.Logging;
using Stigar.Core.Services;

namespace Stigar.Workers;

/// <summary>
/// Timer trigger (Tuesday 03:00 UTC): enqueues one non-urgent scrape job per stale organizer.
/// To force an immediate full re-scrape of all organizers, POST to
/// <c>POST /api/manage/queue-all-scrape-jobs</c> instead.
/// </summary>
public class QueueAllScrapeJobs(
    IOrganizerStore raceOrganizerClient,
    RaceDiscoveryService discoveryService,
    ILogger<QueueAllScrapeJobs> logger) : IRecurringJob
{
    public string JobId => "QueueAllScrapeJobs";
    public string Cron => "0 0 3 * * 2";

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var cutoffUtc = DateTime.UtcNow.Subtract(RaceDiscoveryService.AutomaticScrapeFreshnessWindow);
        var ids = await raceOrganizerClient.GetIdsDueForAutomaticScrapeAsync(cutoffUtc, cancellationToken);
        logger.LogInformation("Scheduled trigger: queuing {Count} automatic scrape jobs for organizers not scraped since {CutoffUtc}", ids.Count, cutoffUtc);
        await discoveryService.EnqueueScrapeMessagesAsync(ids.ToHashSet(StringComparer.OrdinalIgnoreCase), cancellationToken, isUrgent: false);
    }

}
