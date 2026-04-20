using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Backend;

public class DiscoverDuvRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverDuvRaces> logger)
{
    [Function(nameof(DiscoverDuvRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("duv", jobs, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(CancellationToken cancellationToken)
    {
        const int MaxPages = 10;
        var httpClient = httpClientFactory.CreateClient();
        var jobsByUrl = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);

        for (int page = 1; page <= MaxPages; page++)
        {
            var pageUrl = page == 1
                ? DuvDiscoveryAgent.CalendarUrl
                : new UriBuilder(DuvDiscoveryAgent.CalendarUrl) { Query = $"page={page}" }.Uri;

            try
            {
                var html = await httpClient.GetStringAsync(pageUrl, cancellationToken);
                var jobs = DuvDiscoveryAgent.ParseCalendarPage(html, DuvDiscoveryAgent.CalendarUrl);
                if (jobs.Count == 0) break;

                foreach (var job in jobs)
                {
                    if (job.WebsiteUrl is not null)
                        jobsByUrl.TryAdd(job.WebsiteUrl.AbsoluteUri, job);
                }

                if (!html.Contains($"calendar.php?&page={page + 1}", StringComparison.OrdinalIgnoreCase))
                    break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "DUV: failed to fetch calendar page {Page}", page);
                break;
            }
        }

        logger.LogInformation("DUV: discovered {Count} calendar events", jobsByUrl.Count);

        var enriched = new List<ScrapeJob>(jobsByUrl.Count);
        foreach (var job in jobsByUrl.Values)
        {
            if (job.WebsiteUrl is null)
            {
                enriched.Add(job);
                continue;
            }
            try
            {
                enriched.Add(await DuvDiscoveryAgent.EnrichJobAsync(httpClient, job, cancellationToken));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "DUV: failed to fetch event detail page for {Url}", job.WebsiteUrl);
                enriched.Add(job);
            }
        }

        return enriched.ToArray();
    }
}
