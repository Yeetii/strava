using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;
using System.Net;

namespace Backend;

public class DiscoverTraceDeTrailRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    RaceOrganizerClient organizerClient,
    ILogger<DiscoverTraceDeTrailRaces> logger)
{
    private static readonly Uri CalendarUrl = new("https://tracedetrail.fr/event/getEventsCalendar/all/all/all");
    private const string CalendarReferer = "https://tracedetrail.fr/en/calendar";
    private const int MaxRequestsPerRun = 450;

    [Function(nameof(DiscoverTraceDeTrailRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("tracedetrail", jobs, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var jobsByUrl = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);
        var now = DateTime.UtcNow;
        var knownTraceDeTrailIds = await organizerClient.FetchKnownTraceDeTrailIdsAsync(cancellationToken);
        int skippedKnownEvents = 0;
        var requestsRemaining = MaxRequestsPerRun;

        foreach (var monthOffset in Enumerable.Range(-12, 25))
        {
            if (requestsRemaining <= 0) break;

            var date = now.AddMonths(monthOffset);
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, CalendarUrl)
                {
                    Content = new FormUrlEncodedContent(
                    [
                        new KeyValuePair<string, string>("month", date.Month.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new KeyValuePair<string, string>("year", date.Year.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    ]),
                };
                request.Headers.Referrer = new Uri(CalendarReferer);

                using var response = await httpClient.SendAsync(request, cancellationToken);
                requestsRemaining--;

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    logger.LogWarning("TraceDeTrail: calendar returned 429 for {Month}/{Year}, stopping", date.Month, date.Year);
                    break;
                }

                if (!response.IsSuccessStatusCode)
                {
                    logger.LogWarning("TraceDeTrail: calendar returned {Status} for {Month}/{Year}", response.StatusCode, date.Month, date.Year);
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                    continue;
                }

                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                foreach (var job in RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(json))
                {
                    var eventId = job.ExternalIds?.TryGetValue("tracedetrailEventId", out var tId) == true ? tId : null;
                    var itraEventId = job.ExternalIds?.TryGetValue("itraEventId", out var iId) == true ? iId : null;

                    if ((!string.IsNullOrWhiteSpace(eventId) && knownTraceDeTrailIds.Contains(eventId!))
                        || (!string.IsNullOrWhiteSpace(itraEventId) && knownTraceDeTrailIds.Contains(itraEventId!)))
                    {
                        skippedKnownEvents++;
                        continue;
                    }

                    var key = job.TraceDeTrailItraUrls?.FirstOrDefault()?.AbsoluteUri ?? job.TraceDeTrailEventUrl?.AbsoluteUri;
                    if (key is not null)
                        jobsByUrl.TryAdd(key, job);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "TraceDeTrail: failed to fetch calendar for {Month}/{Year}", date.Month, date.Year);
            }
        }

        logger.LogInformation("TraceDeTrail: discovered {Count} unique jobs, skipped {Skipped} already-known events, requests remaining {Remaining}, enriching with site URLs…",
            jobsByUrl.Count, skippedKnownEvents, requestsRemaining);

        var enriched = new List<ScrapeJob>(jobsByUrl.Count);
        var stoppedEventEnrichment = false;
        var resolved = 0;

        foreach (var job in jobsByUrl.Values)
        {
            if (job.TraceDeTrailEventUrl is null || stoppedEventEnrichment || requestsRemaining <= 0)
                continue;

            using var response = await httpClient.GetAsync(job.TraceDeTrailEventUrl, cancellationToken);
            requestsRemaining--;

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
            {
                stoppedEventEnrichment = true;
                logger.LogWarning("TraceDeTrail: received 429 from {Url}, stopping event page enrichment", job.TraceDeTrailEventUrl);
                continue;
            }

            if (!response.IsSuccessStatusCode)
            {
                logger.LogDebug("TraceDeTrail: {Status} fetching {Url}", response.StatusCode, job.TraceDeTrailEventUrl);
                continue;
            }

            var html = await response.Content.ReadAsStringAsync(cancellationToken);
            var siteUrl = RaceHtmlScraper.ExtractRaceSiteUrl(html, job.TraceDeTrailEventUrl);
            var location = RaceHtmlScraper.ExtractTraceDeTrailLocation(html);

            if (siteUrl is not null) resolved++;

            var courses = RaceHtmlScraper.ExtractTraceDeTrailCourses(html);
            if (courses.Count > 0)
            {
                var itraUrlByTraceId = new Dictionary<int, Uri>();
                if (job.TraceDeTrailItraUrls is not null)
                {
                    foreach (var url in job.TraceDeTrailItraUrls)
                    {
                        var seg = url.Segments.LastOrDefault()?.TrimEnd('/');
                        if (int.TryParse(seg, out var id))
                            itraUrlByTraceId[id] = url;
                    }
                }

                enriched.AddRange(courses.Select(c =>
                {
                    IReadOnlyList<Uri>? itraUrls = null;
                    if (c.TraceId is not null && itraUrlByTraceId.TryGetValue(c.TraceId.Value, out var itraUrl))
                        itraUrls = [itraUrl];

                    return job with
                    {
                        Name = c.Name ?? job.Name,
                        Distance = c.Distance ?? job.Distance,
                        ElevationGain = c.ElevationGain ?? job.ElevationGain,
                        ItraPoints = c.ItraPoints ?? job.ItraPoints,
                        Location = location ?? job.Location,
                        WebsiteUrl = siteUrl ?? job.WebsiteUrl,
                        TraceDeTrailItraUrls = itraUrls ?? job.TraceDeTrailItraUrls,
                    };
                }));
                continue;
            }

            var itraPoints = RaceHtmlScraper.ExtractTraceDeTrailItraPoints(html);
            var elevationGain = RaceHtmlScraper.ExtractTraceDeTrailElevationGain(html);
            enriched.Add(job with
            {
                WebsiteUrl = siteUrl ?? job.WebsiteUrl,
                Location = location ?? job.Location,
                ItraPoints = itraPoints ?? job.ItraPoints,
                ElevationGain = elevationGain ?? job.ElevationGain,
            });
        }

        logger.LogInformation("TraceDeTrail: resolved {Resolved}/{Total} event site URLs", resolved, jobsByUrl.Count);
        return enriched.ToArray();
    }
}
