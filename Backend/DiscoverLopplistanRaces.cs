using System.Text.RegularExpressions;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Backend;

public class DiscoverLopplistanRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverLopplistanRaces> logger)
{
    private static readonly Uri CalendarUrl = new("https://lopplistan.se/sverige/trail/");
    private const int MaxPages = 50;

    [Function(nameof(DiscoverLopplistanRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        await discoveryService.DiscoverAndWriteAsync("lopplistan", jobs, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var httpClient = httpClientFactory.CreateClient();
            var jobs = new List<ScrapeJob>();

            for (var page = 1; page <= MaxPages; page++)
            {
                var pageUrl = page == 1 ? CalendarUrl : new Uri(CalendarUrl, $"?page={page}");
                logger.LogInformation("Lopplistan: fetching page {Page} from {Url}", page, pageUrl);

                using var response = await httpClient.GetAsync(pageUrl, cancellationToken);
                response.EnsureSuccessStatusCode();
                var html = await response.Content.ReadAsStringAsync(cancellationToken);

                var pageJobs = RaceScrapeDiscovery.ParseLopplistanTrailPage(html, pageUrl);
                if (pageJobs.Count == 0)
                {
                    logger.LogInformation("Lopplistan: page {Page} returned no jobs, stopping.", page);
                    break;
                }

                jobs.AddRange(pageJobs);
                if (!HasNextPage(html, page))
                {
                    logger.LogInformation("Lopplistan: no next page link found on page {Page}, stopping.", page);
                    break;
                }
            }

            var resolvedJobs = await ResolveLopplistanEventWebsiteUrlsAsync(httpClient, jobs, cancellationToken);
            logger.LogInformation("Lopplistan: discovered {Count} events", resolvedJobs.Count);
            return resolvedJobs;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Lopplistan: failed to fetch or parse trail page");
            return [];
        }
    }

    private static bool HasNextPage(string html, int currentPage)
    {
        var matches = Regex.Matches(html, "href\\s*=\\s*['\"](?:\\?page=|/sverige/trail/\\?page=)(\\d+)['\"]", RegexOptions.IgnoreCase);
        foreach (Match match in matches)
        {
            if (match.Groups.Count < 2)
                continue;

            if (int.TryParse(match.Groups[1].Value, out var pageNumber) && pageNumber > currentPage)
                return true;
        }

        return false;
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> ResolveLopplistanEventWebsiteUrlsAsync(HttpClient httpClient, IReadOnlyCollection<ScrapeJob> jobs, CancellationToken cancellationToken)
    {
        if (jobs.Count == 0)
            return jobs;

        using var semaphore = new SemaphoreSlim(4);
        var resolvedTasks = jobs.Select(async job =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                return await ResolveLopplistanEventWebsiteUrlAsync(httpClient, job, cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        });

        return await Task.WhenAll(resolvedTasks);
    }

    private async Task<ScrapeJob> ResolveLopplistanEventWebsiteUrlAsync(HttpClient httpClient, ScrapeJob job, CancellationToken cancellationToken)
    {
        if (job.WebsiteUrl is null || !job.WebsiteUrl.Host.Contains("lopplistan.se", StringComparison.OrdinalIgnoreCase))
            return job;

        try
        {
            using var response = await httpClient.GetAsync(job.WebsiteUrl, cancellationToken);
            response.EnsureSuccessStatusCode();
            var html = await response.Content.ReadAsStringAsync(cancellationToken);
            var resolvedUrl = RaceScrapeDiscovery.ExtractLopplistanEventWebsiteUrl(html, job.WebsiteUrl);
            if (resolvedUrl is null || resolvedUrl == job.WebsiteUrl)
                return job;

            return job with { WebsiteUrl = resolvedUrl, LopplistanEventUrl = job.WebsiteUrl };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Lopplistan: failed to resolve event website for {EventPageUrl}", job.WebsiteUrl);
            return job;
        }
    }
}
