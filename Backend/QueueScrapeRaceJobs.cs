using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;

namespace Backend;

// Single timer-triggered function that runs all race-data discoveries and enqueues
// the results onto the shared scrapeRace queue.
// Sources are fetched in parallel where possible; jobs are sent in a single batch.
// Priority when the worker combines results: UTMB → TraceDeTrail → Runagain → Loppkartan.
public class QueueScrapeRaceJobs(
    IHttpClientFactory httpClientFactory,
    ServiceBusClient serviceBusClient,
    ILogger<QueueScrapeRaceJobs> logger)
{
    private static readonly Uri UtmbApiUrl = new("https://api.utmb.world/search/races?lang=en&limit=400");
    private static readonly Uri TraceDeTrailCalendarUrl = new("https://tracedetrail.fr/event/getEventsCalendar/all/all/all");
    private const string TraceDeTrailCalendarReferer = "https://tracedetrail.fr/en/calendar";
    private static readonly Uri RunagainBaseUrl = new("https://runagain.com/");
    private static readonly string[] RunagainRaceTypes = ["Stiløp"];

    private static readonly Uri LoppkartanMarkersUrl = new("https://www.loppkartan.se/markers-se.json");

    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.ScrapeRace);
    public async Task Run(
        [TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var all = new List<ScrapeJob>();

        // Fetch all sources (TraceDeTrail is sequential across months; others are single requests).
        all.AddRange(await FetchUtmbJobsAsync(httpClient, cancellationToken));
        all.AddRange(await FetchTraceDeTrailJobsAsync(httpClient, cancellationToken));
        all.AddRange(await FetchRunagainJobsAsync(httpClient, cancellationToken));
        all.AddRange(await FetchLoppkartanJobsAsync(httpClient, cancellationToken));

        logger.LogInformation("QueueScrapeRaceJobs: {Count} total jobs discovered across all sources", all.Count);

        const int ChunkSize = 100;
        var messages = all
            .Select((j, i) => new ServiceBusMessage(BinaryData.FromObjectAsJson(j))
            {
                ContentType = "application/json",
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(i * 5)
            })
            .ToList();

        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _sender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        logger.LogInformation("QueueScrapeRaceJobs: enqueued {Count} messages", messages.Count);
    }

    // ── Per-source fetch methods ──────────────────────────────────────────────

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchUtmbJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        try
        {
            var json = await httpClient.GetStringAsync(UtmbApiUrl, cancellationToken);
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

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchTraceDeTrailJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        var jobsByUrl = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);
        var now = DateTime.UtcNow;

        for (int monthOffset = 0; monthOffset <= 12; monthOffset++)
        {
            var date = now.AddMonths(monthOffset);
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, TraceDeTrailCalendarUrl)
                {
                    Content = new FormUrlEncodedContent(
                    [
                        new KeyValuePair<string, string>("month", date.Month.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new KeyValuePair<string, string>("year", date.Year.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    ]),
                };
                request.Headers.Referrer = new Uri(TraceDeTrailCalendarReferer);

                var response = await httpClient.SendAsync(request, cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    logger.LogWarning("TraceDeTrail: calendar returned {Status} for {Month}/{Year}", response.StatusCode, date.Month, date.Year);
                    continue;
                }

                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                foreach (var job in RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(json))
                {
                    // Deduplicate by ITRA URL (primary key for TraceDeTrail jobs).
                    var key = job.TraceDeTrailItraUrl?.AbsoluteUri ?? job.TraceDeTrailEventUrl?.AbsoluteUri;
                    if (key is not null)
                        jobsByUrl.TryAdd(key, job);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "TraceDeTrail: failed to fetch calendar for {Month}/{Year}", date.Month, date.Year);
            }
        }

        logger.LogInformation("TraceDeTrail: discovered {Count} unique jobs", jobsByUrl.Count);
        return [.. jobsByUrl.Values];
    }

    // Paginates all race types on RunAgain (https://runagain.com/find-event?race_type={type}&p={n})
    // until an empty page is returned.  Yields one ScrapeJob per discovered event URL.
    private async Task<IReadOnlyCollection<ScrapeJob>> FetchRunagainJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        var seenUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var jobs = new List<ScrapeJob>();

        foreach (var raceType in RunagainRaceTypes)
        {
            for (int page = 1; ; page++)
            {
                var listingUrl = new Uri(RunagainBaseUrl, $"find-event?race_type={Uri.EscapeDataString(raceType)}&p={page}");
                string html;
                try
                {
                    html = await httpClient.GetStringAsync(listingUrl, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    logger.LogWarning(ex, "RunAgain: failed to fetch listing page {Url}", listingUrl);
                    break;
                }

                var eventLinks = RaceHtmlScraper.ExtractRunagainEventLinks(html, RunagainBaseUrl);
                if (eventLinks.Count == 0)
                    break;

                var added = 0;
                foreach (var eventUrl in eventLinks)
                {
                    if (seenUrls.Add(eventUrl.AbsoluteUri))
                    {
                        jobs.Add(new ScrapeJob(RunagainUrl: eventUrl));
                        added++;
                    }
                }

                // No new events on this page — stop paginating this race type.
                if (added == 0) break;
            }
        }

        logger.LogInformation("RunAgain: discovered {Count} unique events", jobs.Count);
        return jobs;
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchLoppkartanJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        try
        {
            var json = await httpClient.GetStringAsync(LoppkartanMarkersUrl, cancellationToken);
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
