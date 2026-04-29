using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Services;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Backend;

public class RaceDiscoveryService(ServiceBusClient serviceBusClient, BlobOrganizerStore organizerClient, ILoggerFactory loggerFactory)
{
    internal static readonly TimeSpan AutomaticScrapeFreshnessWindow = TimeSpan.FromDays(6);
    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.ScrapeRace);
    private readonly ServiceBusSender _discoverySender = serviceBusClient.CreateSender(ServiceBusConfig.RaceDiscoveryJobs);
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
                var message = BuildScrapeServiceBusMessage(new ScrapeRaceMessage(key, isUrgent));
                return message;
            })
            .ToList();

        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _sender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        _logger.LogInformation("Enqueued {Count} {Urgency} scrape messages", messages.Count, isUrgent ? "urgent" : "automatic");
    }

    public static ServiceBusMessage BuildScrapeServiceBusMessage(ScrapeRaceMessage message)
    {
        var body = BinaryData.FromString(JsonSerializer.Serialize(message, JsonSerializerOptions));
        return new ServiceBusMessage(body)
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
        var serviceBusMessage = BuildDiscoveryServiceBusMessage(message);
        if (delay.HasValue)
            serviceBusMessage.ScheduledEnqueueTime = DateTimeOffset.UtcNow.Add(delay.Value);

        await _discoverySender.SendMessageAsync(serviceBusMessage, cancellationToken);
        _logger.LogInformation("Enqueued race discovery message for {Agent} page {Page}", message.Agent, message.Page);
    }

    public static ServiceBusMessage BuildDiscoveryServiceBusMessage(RaceDiscoveryMessage message)
    {
        var body = BinaryData.FromString(JsonSerializer.Serialize(message, JsonSerializerOptions));
        return new ServiceBusMessage(body)
        {
            ContentType = "application/json",
            MessageId = $"{message.Agent}:{message.Page ?? 1}:{Guid.NewGuid()}"
        };
    }
}

public sealed record RaceDiscoveryMessage(string Agent, int? Page = null)
{
    public int CurrentPage => Page.GetValueOrDefault(1);
}

public sealed record ScrapeRaceMessage(string OrganizerKey, bool IsUrgent = false);
