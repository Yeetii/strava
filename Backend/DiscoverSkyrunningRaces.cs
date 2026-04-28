using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Net;

namespace Backend;

public class DiscoverSkyrunningRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverSkyrunningRaces> logger)
{
    [Function(nameof(DiscoverSkyrunningRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        await discoveryService.EnqueueDiscoveryMessageAsync(new RaceDiscoveryMessage("skyrunning"), delay: null, cancellationToken);
    }

    public async Task<bool> ProcessPageAsync(int page, CancellationToken cancellationToken)
    {
        var result = await FetchPageJobsAsync(page, cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("skyrunning", result, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);
        return false;
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchPageJobsAsync(int page, CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        using var response = await httpClient.GetAsync(SkyrunningDiscoveryAgent.CalendarUrl, cancellationToken);
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
            throw new HttpRequestException("Skyrunning calendar returned 429", null, response.StatusCode);

        response.EnsureSuccessStatusCode();
        var html = await response.Content.ReadAsStringAsync(cancellationToken);
        var jobs = SkyrunningDiscoveryAgent.ParseCalendarPage(html, SkyrunningDiscoveryAgent.CalendarUrl);
        var enriched = new List<ScrapeJob>(jobs.Count);

        foreach (var job in jobs)
        {
            if (job.WebsiteUrl is null)
            {
                enriched.Add(job);
                continue;
            }

            enriched.Add(await SkyrunningDiscoveryAgent.EnrichJobAsync(httpClient, job, cancellationToken));
        }

        logger.LogInformation("Skyrunning: discovered {Count} events", enriched.Count);
        return enriched;
    }
}
