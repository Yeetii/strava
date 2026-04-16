using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;

namespace Backend;

public class QueueScrapeLoppkartanJobs(
    IHttpClientFactory httpClientFactory,
    ServiceBusClient serviceBusClient,
    ILogger<QueueScrapeLoppkartanJobs> logger)
{
    private static readonly Uri MarkersUrl = new("https://www.loppkartan.se/markers-se.json");
    private readonly ServiceBusSender _upsertSender = serviceBusClient.CreateSender(ServiceBusConfig.UpsertLoppkartanRace);

    [Function(nameof(QueueScrapeLoppkartanJobs))]
    public async Task Run(
        [TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var json = await httpClient.GetStringAsync(MarkersUrl, cancellationToken);
        var targets = RaceScrapeDiscovery.ParseLoppkartanMarkers(json);

        logger.LogInformation("Loppkartan: discovered {Count} unique markers", targets.Count);

        var messages = targets
            .Select((t, i) => new ServiceBusMessage(BinaryData.FromObjectAsJson(t))
            {
                ContentType = "application/json",
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(i * 10)
            })
            .ToList();

        const int ChunkSize = 100;
        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _upsertSender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        logger.LogInformation("Loppkartan: enqueued {Count} race messages", messages.Count);
    }
}
