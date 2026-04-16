using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;

namespace Backend;

public class QueueScrapeUtmbJobs(
    IHttpClientFactory httpClientFactory,
    ServiceBusClient serviceBusClient,
    ILogger<QueueScrapeUtmbJobs> logger)
{
    private static readonly Uri UtmbSearchApiUrl = new("https://api.utmb.world/search/races?lang=en&limit=400");
    private readonly ServiceBusSender _upsertSender = serviceBusClient.CreateSender(ServiceBusConfig.UpsertUtmbRace);

    [Function(nameof(QueueScrapeUtmbJobs))]
    public async Task Run(
        [TimerTrigger("0 0 1 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var json = await httpClient.GetStringAsync(UtmbSearchApiUrl, cancellationToken);
        var pages = RaceScrapeDiscovery.ParseUtmbRacePages(json);

        logger.LogInformation("UTMB: discovered {Count} race pages", pages.Count);

        var messages = pages
            .Select((p, i) => new ServiceBusMessage(BinaryData.FromObjectAsJson(p))
            {
                ContentType = "application/json",
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(i * 5)
            })
            .ToList();

        const int ChunkSize = 100;
        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _upsertSender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        logger.LogInformation("UTMB: enqueued {Count} race page messages", messages.Count);
    }
}

