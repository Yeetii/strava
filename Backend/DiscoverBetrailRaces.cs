using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Net;

namespace Backend;

public class DiscoverBetrailRaces(
    RaceDiscoveryService discoveryService,
    ILogger<DiscoverBetrailRaces> logger)
{
    // Debug-only throttle: limit discovery volume for faster local iteration.
    private const bool DebugLimitEventsEnabled = false;
    private const int DebugMaxEvents = 10;

    private static readonly Uri ApiBaseUrl = new("https://www.betrail.run/api/events-drizzle");
    private static readonly Uri CalendarUrl = new("https://www.betrail.run/en/calendar/all");

    [Function(nameof(DiscoverBetrailRaces))]
    public async Task Run([TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo, CancellationToken cancellationToken)
    {
        var jobs = await FetchJobsAsync(cancellationToken);
        var keys = await discoveryService.DiscoverAndWriteAsync("betrail", jobs, cancellationToken);
        await discoveryService.EnqueueScrapeMessagesAsync(keys, cancellationToken);
    }

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchJobsAsync(CancellationToken cancellationToken)
    {
        using var handler = new HttpClientHandler
        {
            CookieContainer = new CookieContainer(),
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate | DecompressionMethods.Brotli
        };
        using var httpClient = CreateBetrailClient(handler);

        // Warm up Cloudflare/session cookies on the public calendar page before querying the API.
        using (var warmup = new HttpRequestMessage(HttpMethod.Get, CalendarUrl))
        {
            warmup.Headers.Referrer = CalendarUrl;
            var warmupResponse = await httpClient.SendAsync(warmup, cancellationToken);
            if (!warmupResponse.IsSuccessStatusCode)
                logger.LogInformation("BeTrail: calendar warm-up returned {Status}", warmupResponse.StatusCode);
        }

        // BeTrailUrl is unique per race so it's the natural dedupe key — one event's races
        // each get their own URL via the /{race.alias} suffix.
        var jobsByKey = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);

        var today = DateOnly.FromDateTime(DateTime.UtcNow);
        var after = today.AddDays(-1);
        var before = today.AddYears(1);
        const int pageSize = 200;

        for (int offset = 0; ; offset += pageSize)
        {
            var url = BuildApiUrl(after, before, offset);

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Referrer = CalendarUrl;
                request.Headers.Add("sec-fetch-site", "same-origin");
                request.Headers.Add("sec-fetch-mode", "cors");
                request.Headers.Add("sec-fetch-dest", "empty");

                using var response = await httpClient.SendAsync(request, cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    logger.LogWarning("BeTrail: events API returned {Status} (offset {Offset})", response.StatusCode, offset);
                    break;
                }

                var contentType = response.Content.Headers.ContentType?.MediaType;
                var json = await response.Content.ReadAsStringAsync(cancellationToken);
                var page = RaceScrapeDiscovery.ParseBeTrailEvents(json);
                if (offset == 0 && page.Count == 0)
                {
                    logger.LogWarning(
                        "BeTrail: parsed 0 events from first page (content-type: {ContentType}, payload-prefix: {Prefix})",
                        contentType ?? "unknown",
                        json.Length > 200 ? json[..200] : json);
                }
                if (page.Count == 0)
                    break;

                foreach (var job in page)
                {
                    var key = job.BetrailUrl?.AbsoluteUri
                        ?? job.WebsiteUrl?.AbsoluteUri
                        ?? $"{job.Name}|{job.Date}|{job.Distance}";
                    jobsByKey.TryAdd(key, job);
                }

                // Stop paging when the API returns fewer records than requested.
                if (page.Count < pageSize)
                    break;

                // BeTrail can return the full dataset at offset=0 (unpaginated).
                // In that case, calling offset>0 may return 500 with no additional value.
                if (offset == 0 && page.Count > pageSize)
                    break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "BeTrail: failed to fetch events (offset {Offset})", offset);
                break;
            }
        }

        var discoveredJobs = jobsByKey.Values.ToArray();
        logger.LogInformation("BeTrail: discovered {Count} unique races", discoveredJobs.Length);

        if (DebugLimitEventsEnabled && discoveredJobs.Length > DebugMaxEvents)
        {
            discoveredJobs = discoveredJobs.Take(DebugMaxEvents).ToArray();
            logger.LogInformation("BeTrail: debug limit enabled, processing only {Count} races", discoveredJobs.Length);
        }

        return discoveredJobs;
    }

    private static Uri BuildApiUrl(DateOnly after, DateOnly before, int offset)
    {
        var query =
            $"after={FormatDate(after)}&before={FormatDate(before)}&scope=full&predicted=1&length=full&offset={offset}&country=all&forAddition=false&lang=en";
        return new UriBuilder(ApiBaseUrl) { Query = query }.Uri;
    }

    private static string FormatDate(DateOnly date)
        => string.Create(CultureInfo.InvariantCulture, $"{date.Year}-{date.Month}-{date.Day}");

    private static HttpClient CreateBetrailClient(HttpClientHandler handler)
    {
        var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(60) };
        client.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36");
        client.DefaultRequestHeaders.Accept.ParseAdd("application/json, text/plain, */*");
        client.DefaultRequestHeaders.AcceptLanguage.ParseAdd("en-US,en;q=0.9");
        client.DefaultRequestHeaders.Add("origin", "https://www.betrail.run");
        client.DefaultRequestHeaders.Add("x-requested-with", "XMLHttpRequest");
        return client;
    }
}
