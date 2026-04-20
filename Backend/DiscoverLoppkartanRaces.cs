using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Backend;

public class DiscoverLoppkartanRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverLoppkartanRaces> logger)
{
    private static readonly Uri MarkersUrl = new("https://www.loppkartan.se/markers-se.json");

    [Function(nameof(DiscoverLoppkartanRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("loppkartan", jobs, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(CancellationToken cancellationToken)
    {
        try
        {
            var json = await httpClientFactory.CreateClient().GetStringAsync(MarkersUrl, cancellationToken);
            var jobs = RaceScrapeDiscovery.ParseLoppkartanMarkers(json);
            logger.LogInformation("Loppkartan: discovered {Count} markers", jobs.Count);
            return jobs;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Loppkartan: failed to fetch markers");
            return [];
        }
    }
}
