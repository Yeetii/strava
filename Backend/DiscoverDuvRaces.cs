using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Net;

namespace Backend;

public class DiscoverDuvRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverDuvRaces> logger)
{
    [Function(nameof(DiscoverDuvRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        await discoveryService.EnqueueDiscoveryMessageAsync(new RaceDiscoveryMessage("duv"), delay: null, cancellationToken);
    }

    public async Task<bool> ProcessPageAsync(int page, CancellationToken cancellationToken)
    {
        var result = await FetchPageJobsAsync(page, cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("duv", result.Jobs, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);

        return result.HasNextPage;
    }

    private async Task<(IReadOnlyCollection<ScrapeJob> Jobs, bool HasNextPage)> FetchPageJobsAsync(int page, CancellationToken cancellationToken)
    {
        const int MaxPages = 10;
        var httpClient = httpClientFactory.CreateClient();
        var jobsByUrl = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);
        var pageUrl = BuildCalendarPageUrl(page);

        using (var response = await httpClient.GetAsync(pageUrl, cancellationToken))
        {
            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                throw new HttpRequestException($"DUV calendar returned 429 for page {page}", null, response.StatusCode);

            response.EnsureSuccessStatusCode();

            var html = await response.Content.ReadAsStringAsync(cancellationToken);
            var jobs = DuvDiscoveryAgent.ParseCalendarPage(html, DuvDiscoveryAgent.CalendarUrl);

            foreach (var job in jobs)
            {
                if (job.WebsiteUrl is not null)
                    jobsByUrl.TryAdd(job.WebsiteUrl.AbsoluteUri, job);
            }

            var hasNextPage = page < MaxPages
                && jobs.Count > 0
                && html.Contains($"calendar.php?&page={page + 1}", StringComparison.OrdinalIgnoreCase);

            logger.LogInformation("DUV: discovered {Count} calendar events on page {Page}", jobsByUrl.Count, page);
            return (await EnrichJobsAsync(httpClient, jobsByUrl.Values, cancellationToken), hasNextPage);
        }
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> EnrichJobsAsync(
        HttpClient httpClient,
        IEnumerable<ScrapeJob> jobs,
        CancellationToken cancellationToken)
    {
        var enriched = new List<ScrapeJob>();
        foreach (var job in jobs)
        {
            if (job.WebsiteUrl is null)
            {
                enriched.Add(job);
                continue;
            }
            enriched.Add(await DuvDiscoveryAgent.EnrichJobAsync(httpClient, job, cancellationToken));
        }

        return enriched.ToArray();
    }

    private static Uri BuildCalendarPageUrl(int page)
        => page <= 1
            ? DuvDiscoveryAgent.CalendarUrl
            : new UriBuilder(DuvDiscoveryAgent.CalendarUrl) { Query = $"page={page}" }.Uri;
}
