using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
namespace Backend;

public class DiscoverTrailrunningSwedenRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverTrailrunningSwedenRaces> logger)
{
    private static readonly Uri CalendarUrl = new("https://trailrunningsweden.se/trailkalendern/");
    private const string UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36";
    private const bool DebugLimitEventsEnabled = false;
    private const int DebugMaxEvents = 10;

    [Function(nameof(DiscoverTrailrunningSwedenRaces))]
    public async Task Run([TimerTrigger("0 0 3 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        await discoveryService.DiscoverAndWriteAsync("trailrunningsweden", jobs, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent);
        httpClient.DefaultRequestHeaders.Accept.ParseAdd("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
        httpClient.DefaultRequestHeaders.AcceptLanguage.ParseAdd("sv-SE,sv;q=0.9,en-US;q=0.8,en;q=0.7");
        httpClient.DefaultRequestHeaders.Referrer = new Uri("https://trailrunningsweden.se/");

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, CalendarUrl);
            request.Headers.Referrer = new Uri("https://trailrunningsweden.se/");

            using var response = await httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();
            var html = await response.Content.ReadAsStringAsync(cancellationToken);
            var jobs = RaceScrapeDiscovery.ParseTrailrunningSwedenCalendarPage(html);
            jobs = await ResolveTrailrunningSwedenEventWebsitesAsync(httpClient, jobs, cancellationToken);
            jobs = jobs.Select(job => job with { RaceType = "trail" }).ToList();

            if (DebugLimitEventsEnabled && jobs.Count > DebugMaxEvents)
            {
                jobs = [.. jobs.Take(DebugMaxEvents)];
                logger.LogInformation("TrailrunningSweden: debug limit enabled, processing only {Count} events", jobs.Count);
            }

            logger.LogInformation("TrailrunningSweden: discovered {Count} events", jobs.Count);
            return jobs;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "TrailrunningSweden: failed to fetch or parse calendar page");
            return [];
        }
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> ResolveTrailrunningSwedenEventWebsitesAsync(HttpClient httpClient, IReadOnlyCollection<ScrapeJob> jobs, CancellationToken cancellationToken)
    {
        if (jobs.Count == 0)
            return jobs;

        using var semaphore = new SemaphoreSlim(4);
        var resolvedTasks = jobs.Select(async job =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                return await ResolveTrailrunningSwedenEventWebsiteUrlAsync(httpClient, job, cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        });

        return await Task.WhenAll(resolvedTasks);
    }

    private async Task<ScrapeJob> ResolveTrailrunningSwedenEventWebsiteUrlAsync(HttpClient httpClient, ScrapeJob job, CancellationToken cancellationToken)
    {
        if (job.WebsiteUrl is null || !job.WebsiteUrl.AbsolutePath.Contains("/events/", StringComparison.OrdinalIgnoreCase))
            return job with { TrailrunningSwedenEventUrl = job.WebsiteUrl };

        var resolvedUrl = await FetchTrailrunningSwedenEventWebsiteUrlAsync(httpClient, job.WebsiteUrl, cancellationToken);
        if (resolvedUrl is null || resolvedUrl == job.WebsiteUrl)
            return job with { TrailrunningSwedenEventUrl = job.WebsiteUrl };

        return job with { WebsiteUrl = resolvedUrl, TrailrunningSwedenEventUrl = job.WebsiteUrl };
    }

    private async Task<Uri?> FetchTrailrunningSwedenEventWebsiteUrlAsync(HttpClient httpClient, Uri eventPageUrl, CancellationToken cancellationToken)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, eventPageUrl);
            request.Headers.Referrer = CalendarUrl;

            using var response = await httpClient.SendAsync(request, cancellationToken);
            response.EnsureSuccessStatusCode();
            var html = await response.Content.ReadAsStringAsync(cancellationToken);
            return RaceScrapeDiscovery.ExtractTrailrunningSwedenEventWebsiteUrl(html, eventPageUrl);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "TrailrunningSweden: failed to resolve event website for {EventPageUrl}", eventPageUrl);
            return null;
        }
    }
}
