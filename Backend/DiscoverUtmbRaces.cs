using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Backend;

public class DiscoverUtmbRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverUtmbRaces> logger)
{
    private static readonly Uri ApiUrl = new("https://api.utmb.world/search/races?lang=en&limit=400");

    [Function(nameof(DiscoverUtmbRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("utmb", jobs, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var json = await httpClientFactory.CreateClient().GetStringAsync(ApiUrl, cancellationToken);
            var jobs = RaceScrapeDiscovery.ParseUtmbRacePages(json);
            logger.LogInformation("UTMB: discovered {Count} races", jobs.Count);
            return jobs;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "UTMB: failed to fetch race list");
            return [];
        }
    }
}
