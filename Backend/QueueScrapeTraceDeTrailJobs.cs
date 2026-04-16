using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;

namespace Backend;

public class QueueScrapeTraceDeTrailJobs(
    IHttpClientFactory httpClientFactory,
    ServiceBusClient serviceBusClient,
    ILogger<QueueScrapeTraceDeTrailJobs> logger)
{
    private static readonly Uri CalendarUrl = new("https://tracedetrail.fr/event/getEventsCalendar/all/all/all");
    private const string CalendarReferer = "https://tracedetrail.fr/en/calendar";
    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.ScrapeRace);

    [Function(nameof(QueueScrapeTraceDeTrailJobs))]
    public async Task Run(
        [TimerTrigger("0 30 1 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var jobsByUrl = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);
        var now = DateTime.UtcNow;

        for (int monthOffset = 0; monthOffset <= 12; monthOffset++)
        {
            var date = now.AddMonths(monthOffset);
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, CalendarUrl)
                {
                    Content = new FormUrlEncodedContent(
                    [
                        new KeyValuePair<string, string>("month", date.Month.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new KeyValuePair<string, string>("year", date.Year.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    ]),
                };
                request.Headers.Referrer = new Uri(CalendarReferer);

                var response = await httpClient.SendAsync(request, cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    logger.LogWarning("TraceDeTrail calendar returned {Status} for {Month}/{Year}", response.StatusCode, date.Month, date.Year);
                    continue;
                }

                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                var jobs = RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(json);
                foreach (var job in jobs)
                {
                    if (job.Url is not null)
                        jobsByUrl.TryAdd(job.Url.AbsoluteUri, job);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Failed to fetch TraceDeTrail calendar for {Month}/{Year}", date.Month, date.Year);
            }
        }

        logger.LogInformation("TraceDeTrail: discovered {Count} unique jobs from calendar", jobsByUrl.Count);

        var messages = jobsByUrl.Values
            .Select((j, i) => new ServiceBusMessage(BinaryData.FromObjectAsJson(j))
            {
                ContentType = "application/json",
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(i * 5)
            })
            .ToList();

        const int ChunkSize = 100;
        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _sender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        logger.LogInformation("TraceDeTrail: enqueued {Count} scrape job messages", messages.Count);
    }
}
