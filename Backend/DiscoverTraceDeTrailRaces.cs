using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Net;

namespace Backend;

public class DiscoverTraceDeTrailRaces(
    IHttpClientFactory httpClientFactory,
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverTraceDeTrailRaces> logger)
{
    private static readonly Uri CalendarUrl = new("https://tracedetrail.fr/event/getEventsCalendar/all/all/all");
    private const string CalendarReferer = "https://tracedetrail.fr/en/calendar";
    private const int StartMonthOffset = -12;
    private const int EndMonthOffset = 12;

    [Function(nameof(DiscoverTraceDeTrailRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        await discoveryService.EnqueueDiscoveryMessageAsync(new RaceDiscoveryMessage("tracedetrail"), delay: null, cancellationToken);
    }

    public async Task<bool> ProcessPageAsync(int page, CancellationToken cancellationToken)
    {
        var jobs = await FetchPageJobsAsync(page, cancellationToken);
        await discoveryService.DiscoverAndWriteAsync("tracedetrail", jobs, cancellationToken);

        return page < TotalPages;
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchPageJobsAsync(int page, CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var jobsByUrl = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);
        var date = MonthForPage(page);

        using var request = new HttpRequestMessage(HttpMethod.Post, CalendarUrl)
        {
            Content = new FormUrlEncodedContent(
            [
                new KeyValuePair<string, string>("month", date.Month.ToString(CultureInfo.InvariantCulture)),
                new KeyValuePair<string, string>("year", date.Year.ToString(CultureInfo.InvariantCulture))
            ]),
        };
        request.Headers.Referrer = new Uri(CalendarReferer);

        using var response = await httpClient.SendAsync(request, cancellationToken);
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
            throw new HttpRequestException($"TraceDeTrail calendar returned 429 for {date.Month}/{date.Year}", null, response.StatusCode);

        response.EnsureSuccessStatusCode();

        var json = await response.Content.ReadAsStringAsync(cancellationToken);
        foreach (var job in RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(json))
        {
            var key = job.TraceDeTrailItraUrls?.FirstOrDefault()?.AbsoluteUri ?? job.TraceDeTrailEventUrl?.AbsoluteUri;
            if (key is not null)
                jobsByUrl.TryAdd(key, job);
        }

        logger.LogInformation("TraceDeTrail: discovered {Count} unique jobs for {Month}/{Year}, enriching with site URLs",
            jobsByUrl.Count, date.Month, date.Year);

        var enriched = new List<ScrapeJob>(jobsByUrl.Count);
        var stoppedEventEnrichment = false;
        var resolved = 0;

        foreach (var job in jobsByUrl.Values)
        {
            if (job.TraceDeTrailEventUrl is null || stoppedEventEnrichment)
                continue;

            using var eventResponse = await httpClient.GetAsync(job.TraceDeTrailEventUrl, cancellationToken);

            if (eventResponse.StatusCode == HttpStatusCode.TooManyRequests)
            {
                throw new HttpRequestException($"TraceDeTrail event page returned 429 for {job.TraceDeTrailEventUrl}", null, response.StatusCode);
            }

            if (!eventResponse.IsSuccessStatusCode)
            {
                logger.LogDebug("TraceDeTrail: {Status} fetching {Url}", eventResponse.StatusCode, job.TraceDeTrailEventUrl);
                continue;
            }

            var html = await eventResponse.Content.ReadAsStringAsync(cancellationToken);
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

    private static int TotalPages => EndMonthOffset - StartMonthOffset + 1;

    internal static DateTime MonthForPage(int page)
        => DateTime.UtcNow.AddMonths(StartMonthOffset + Math.Max(page, 1) - 1);
}
