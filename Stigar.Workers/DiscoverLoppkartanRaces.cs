using Stigar.Core.Services;
using Stigar.Core.Jobs;
using Microsoft.Extensions.Logging;

namespace Stigar.Workers;

public class DiscoverLoppkartanRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverLoppkartanRaces> logger) : IRecurringJob
{
    public string JobId => "DiscoverLoppkartanRaces";
    public string Cron => "0 0 2 * * 1";

    private static readonly Uri MarkersUrl = new("https://www.loppkartan.se/markers-se.json");

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        await discoveryService.DiscoverAndWriteAsync("loppkartan", jobs, cancellationToken);
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
