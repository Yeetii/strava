using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Logging;
using Shared.Services;
using System.Net;

namespace Backend;

/// <summary>
/// Timer trigger (Tuesday 03:00 UTC): enqueues one non-urgent scrape job per stale organizer.
/// To force an immediate full re-scrape of all organizers, POST to
/// <c>POST /api/manage/queue-all-scrape-jobs</c> instead.
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
        logger.LogInformation("Scheduled trigger: queuing {Count} automatic scrape jobs for organizers not scraped since {CutoffUtc}", ids.Count, cutoffUtc);
        await discoveryService.EnqueueScrapeMessagesAsync(ids.ToHashSet(StringComparer.OrdinalIgnoreCase), cancellationToken, isUrgent: false);
    }

    [Function("QueueAllScrapeJobsHttp")]
    public async Task<HttpResponseData> RunHttp(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "manage/queue-all-scrape-jobs")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var allIds = await raceOrganizerClient.GetIdsDueForAutomaticScrapeAsync(DateTime.MaxValue, cancellationToken);
        logger.LogInformation("Manual HTTP trigger: queuing {Count} urgent scrape jobs for all organizers", allIds.Count);
        await discoveryService.EnqueueScrapeMessagesAsync(allIds.ToHashSet(StringComparer.OrdinalIgnoreCase), cancellationToken, isUrgent: true);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteStringAsync($"Queued {allIds.Count} urgent scrape jobs", cancellationToken);
        return response;
    }
}
