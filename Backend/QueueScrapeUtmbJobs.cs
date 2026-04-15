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
        var scrapeTargets = await DiscoverScrapeTargetsAsync(httpClient, cancellationToken);
        var targets = scrapeTargets
            .GroupBy(t => t.GpxUrl.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList();

        logger.LogInformation("UTMB: discovered {Count} unique GPX targets", targets.Count);

        var messages = targets
            .Select(t => new ServiceBusMessage(BinaryData.FromObjectAsJson(t)) { ContentType = "application/json" })
            .ToList();

        const int ChunkSize = 100;
        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _upsertSender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        logger.LogInformation("UTMB: enqueued {Count} race messages", messages.Count);
    }

    private async Task<IReadOnlyCollection<RaceScrapeTarget>> DiscoverScrapeTargetsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        var json = await httpClient.GetStringAsync(UtmbSearchApiUrl, cancellationToken);
        var pages = RaceScrapeDiscovery.ParseUtmbRacePages(json);
        var targets = new List<RaceScrapeTarget>();

        foreach (var page in pages)
        {
            try
            {
                var html = await httpClient.GetStringAsync(page.PageUrl, cancellationToken);
                var gpxUrls = RaceScrapeDiscovery.ExtractGpxUrlsFromHtml(html, page.PageUrl);
                targets.AddRange(gpxUrls.Select(gpxUrl =>
                    new RaceScrapeTarget(gpxUrl, UtmbSearchApiUrl, page.PageUrl, page.Name, page.Distance, page.ElevationGain)));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Failed to discover GPX links from race page {RacePageUrl}", page.PageUrl);
            }
        }

        return targets;
    }
}

