using Shared.Services;
using Shared.Jobs;
using Microsoft.Extensions.Logging;

namespace Backend;

public class DiscoverUtmbRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverUtmbRaces> logger) : IRecurringJob
{
    public string JobId => "DiscoverUtmbRaces";
    public string Cron => "0 0 2 * * 1";

    private static readonly Uri ApiUrl = new("https://api.utmb.world/search/races?lang=en&limit=400");

    public async Task ExecuteAsync(CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        await discoveryService.DiscoverAndWriteAsync("utmb", jobs, cancellationToken);
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
