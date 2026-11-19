using Shared.Jobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Services;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Backend;

public class RaceDiscoveryService(QueueClient queueClient, IOrganizerStore organizerClient, ILoggerFactory loggerFactory)
{
    internal static readonly TimeSpan AutomaticScrapeFreshnessWindow = TimeSpan.FromDays(6);
    private readonly QueueSender _sender = queueClient.CreateSender(QueueConfig.ScrapeRace);
    private readonly QueueSender _discoverySender = queueClient.CreateSender(QueueConfig.RaceDiscoveryJobs);
    private readonly ILogger _logger = loggerFactory.CreateLogger<RaceDiscoveryService>();
    private static readonly JsonSerializerOptions JsonSerializerOptions = new(JsonSerializerDefaults.Web);

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
        _logger.LogInformation("Discovery/{Source}: wrote {Count} organizers to Cosmos", source, items.Count);

        return items.Select(i => i.OrganizerKey).ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    public async Task EnqueueScrapeMessagesAsync(
        IReadOnlySet<string> organizerKeys,
        CancellationToken cancellationToken,
        bool isUrgent = false)
    {
        const int ChunkSize = 100;
        var messages = organizerKeys
            .Select((key, i) =>
            {
                var message = BuildScrapeQueueMessage(new ScrapeRaceMessage(key, isUrgent));
                return message;
            })
            .ToList();

        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _sender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        _logger.LogInformation("Enqueued {Count} {Urgency} scrape messages", messages.Count, isUrgent ? "urgent" : "automatic");
    }

    public static QueueMessage BuildScrapeQueueMessage(ScrapeRaceMessage message)
    {
        var body = JsonSerializer.Serialize(message, JsonSerializerOptions);
        return new QueueMessage(body)
        {
            ContentType = "application/json",
            MessageId = BuildScrapeMessageId(message.OrganizerKey)
        };
    }

    private static string BuildScrapeMessageId(string organizerKey)
    {
        var organizerHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(organizerKey))).ToLowerInvariant();
        return $"scrape:{organizerHash}:{Guid.NewGuid():N}";
    }

    public async Task EnqueueDiscoveryMessageAsync(
        RaceDiscoveryMessage message,
        TimeSpan? delay,
        CancellationToken cancellationToken)
    {
        var scheduledAt = delay.HasValue ? DateTimeOffset.UtcNow.Add(delay.Value) : (DateTimeOffset?)null;
        var queueMessage = BuildDiscoveryQueueMessage(message);
        if (scheduledAt.HasValue)
            queueMessage.ScheduledEnqueueTime = scheduledAt.Value;

        await _discoverySender.SendMessageAsync(queueMessage, cancellationToken);
        _logger.LogInformation("Enqueued race discovery message for {Agent} page {Page}", message.Agent, message.Page);
    }

    public static QueueMessage BuildDiscoveryQueueMessage(RaceDiscoveryMessage message)
    {
        var body = JsonSerializer.Serialize(message, JsonSerializerOptions);
        var queueMessage = new QueueMessage(body)
        {
            ContentType = "application/json",
            MessageId = BuildDiscoveryMessageId(message),
            Subject = $"discovery:{message.Agent.Trim().ToLowerInvariant()}:{message.CurrentPage}"
        };

        if (!string.IsNullOrWhiteSpace(message.Country))
            queueMessage.ApplicationProperties["country"] = message.Country;
        if (message.CountryIndex.HasValue)
            queueMessage.ApplicationProperties["countryIndex"] = message.CountryIndex.Value;
        if (message.CountryPage.HasValue)
            queueMessage.ApplicationProperties["countryPage"] = message.CountryPage.Value;
        if (message.CountryPageCount.HasValue)
            queueMessage.ApplicationProperties["countryPageCount"] = message.CountryPageCount.Value;
        if (message.RacesOnPage.HasValue)
            queueMessage.ApplicationProperties["racesOnPage"] = message.RacesOnPage.Value;

        return queueMessage;
    }

    internal static string BuildDiscoveryMessageId(RaceDiscoveryMessage message)
        => $"discovery:{message.Agent.Trim().ToLowerInvariant()}:{message.CurrentPage}";
}


