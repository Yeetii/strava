using Hangfire;
using Microsoft.Extensions.Logging;
using Shared.Jobs;

namespace Shared.Services;

/// <summary>
/// Builds discovery/scrape payloads and enqueues them as strongly-typed Hangfire
/// jobs. Producers never build a transport envelope; the Workers host supplies
/// the concrete job implementations behind the contracts in Shared.Jobs.
/// </summary>
public class RaceDiscoveryService(
    IBackgroundJobClient jobs,
    IOrganizerStore organizerClient,
    ILoggerFactory loggerFactory)
{
    public static readonly TimeSpan AutomaticScrapeFreshnessWindow = TimeSpan.FromDays(6);
    private readonly ILogger _logger = loggerFactory.CreateLogger<RaceDiscoveryService>();

    public async Task<IReadOnlySet<string>> DiscoverAndWriteAsync(
        string source,
        IReadOnlyCollection<ScrapeJob> jobs,
        CancellationToken cancellationToken)
    {
        var items = jobs
            .Select(j => (Key: RaceScrapeDiscovery.DeriveEventKeyFromJob(j), Job: j))
            .Where(x => x.Key is not null)
            .GroupBy(x => x.Key!.Value.EventKey, StringComparer.OrdinalIgnoreCase)
            .Select(g =>
            {
                var (Key, _) = g.First();
                var discoveries = g.Select(x => x.Job.ToSourceDiscovery()).ToList();
                return (OrganizerKey: g.Key, Key!.Value.CanonicalUrl, Discoveries: discoveries);
            })
            .ToList();

        await organizerClient.WriteDiscoveriesAsync(source, items, cancellationToken);
        _logger.LogInformation("Discovery/{Source}: wrote {Count} organizers", source, items.Count);

        return items.Select(i => i.OrganizerKey).ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public Task EnqueueScrapeMessagesAsync(
        IReadOnlySet<string> organizerKeys,
        CancellationToken cancellationToken,
        bool isUrgent = false)
    {
        foreach (var key in organizerKeys)
            jobs.EnqueueScrapeRace(new ScrapeRaceMessage(key, isUrgent));

        _logger.LogInformation("Enqueued {Count} {Urgency} scrape messages", organizerKeys.Count, isUrgent ? "urgent" : "automatic");
        return Task.CompletedTask;
    }

    public Task EnqueueDiscoveryMessageAsync(
        RaceDiscoveryMessage message,
        TimeSpan? delay,
        CancellationToken cancellationToken)
    {
        jobs.EnqueueRaceDiscovery(message, delay);
        _logger.LogInformation("Enqueued race discovery message for {Agent} page {Page}", message.Agent, message.Page);
        return Task.CompletedTask;
    }

    /// <summary>Stable, human-readable job id for a discovery page (used for observability).</summary>
    internal static string BuildDiscoveryJobId(RaceDiscoveryMessage message)
        => $"discovery:{message.Agent.Trim().ToLowerInvariant()}:{message.CurrentPage}";
}
