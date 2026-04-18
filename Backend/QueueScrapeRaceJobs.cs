using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using System.Text.Json;

namespace Backend;

// Timer-triggered discovery function that fetches race listings from external sources
// and writes one working document per organizer domain to Cosmos (raceOrganizers container).
// Each source writes under its own /discovery/{source} path — no cross-source collisions.
// Multiple events from the same organizer domain are stored as a list under that source key.
// After discovery, enqueues scrape messages (organizer keys) onto the scrapeRace queue.
public class QueueScrapeRaceJobs(
    IHttpClientFactory httpClientFactory,
    ServiceBusClient serviceBusClient,
    RaceOrganizerClient organizerClient,
    ILogger<QueueScrapeRaceJobs> logger)
{
    private static readonly Uri UtmbApiUrl = new("https://api.utmb.world/search/races?lang=en&limit=400");
    private static readonly Uri TraceDeTrailCalendarUrl = new("https://tracedetrail.fr/event/getEventsCalendar/all/all/all");
    private const string TraceDeTrailCalendarReferer = "https://tracedetrail.fr/en/calendar";
    private static readonly Uri RunagainApiUrl = new("https://cloudrun-pgjjiy2k6a-ew.a.run.app/find_runs");
    private static readonly string RunagainFirestoreBatchUrl = "https://firestore.googleapis.com/v1/projects/nestelop-production/databases/(default)/documents:batchGet";
    private static readonly string RunagainFirestoreDocPrefix = "projects/nestelop-production/databases/(default)/documents/RunCollection/";
    private static readonly string[] RunagainRaceTypes = ["Stiløp", "Backyard ultra", "Ultra", "Motbakke"];
    private static readonly string[] RunagainTerrainTypes = ["Terreng"];

    private static readonly Uri LoppkartanMarkersUrl = new("https://www.loppkartan.se/markers-se.json");

    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.ScrapeRace);

    [Function(nameof(QueueScrapeRaceJobs))]
    public async Task Run(
        [TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var allOrganizerKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Fetch each source and write discoveries to Cosmos under /discovery/{source}.
        allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("utmb",
            await FetchUtmbJobsAsync(httpClient, cancellationToken), cancellationToken));
        // allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("tracedetrail",
        //     await FetchTraceDeTrailJobsAsync(httpClient, cancellationToken), cancellationToken));
        allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("runagain",
            await FetchRunagainJobsAsync(httpClient, cancellationToken), cancellationToken));
        allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("loppkartan",
            await FetchLoppkartanJobsAsync(httpClient, cancellationToken), cancellationToken));

        logger.LogInformation("Discovery complete: {Count} unique organizers across all sources", allOrganizerKeys.Count);

        // Enqueue scrape messages — just the organizer key, staggered 5s apart.
        await EnqueueScrapeMessagesAsync(allOrganizerKeys, cancellationToken);
    }

    /// <summary>
    /// Groups discovered ScrapeJobs by organizer key and writes all discoveries for each
    /// organizer as a list under <c>/discovery/{source}</c> in Cosmos.
    /// Returns the set of organizer keys written.
    /// </summary>
    private async Task<IReadOnlySet<string>> DiscoverAndWriteAsync(
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
                var (Key, Job) = g.First();
                var discoveries = g.Select(x => x.Job.ToSourceDiscovery()).ToList();
                return (OrganizerKey: g.Key, Key!.Value.CanonicalUrl, Discoveries: discoveries);
            })
            .ToList();

        await organizerClient.WriteDiscoveriesAsync(source, items, cancellationToken);
        logger.LogInformation("Discovery/{Source}: wrote {Count} organizers to Cosmos", source, items.Count);

        return items.Select(i => i.OrganizerKey).ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private async Task EnqueueScrapeMessagesAsync(
        IReadOnlySet<string> organizerKeys,
        CancellationToken cancellationToken)
    {
        const int ChunkSize = 100;
        var messages = organizerKeys
            .Select((key, i) => new ServiceBusMessage(key)
            {
                ContentType = "text/plain",
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(i * 5)
            })
            .ToList();

        for (int i = 0; i < messages.Count; i += ChunkSize)
            await _sender.SendMessagesAsync(messages.Skip(i).Take(ChunkSize), cancellationToken);

        logger.LogInformation("QueueScrapeRaceJobs: enqueued {Count} scrape messages", messages.Count);
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

        for (int monthOffset = -12; monthOffset <= 12; monthOffset++)
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

        logger.LogInformation("TraceDeTrail: discovered {Count} unique jobs, enriching with site URLs…", jobsByUrl.Count);

        // Fetch each event page to extract the real race website ("Site de la course").
        // When found, set WebsiteUrl so the organizer is keyed by the real domain.
        // Use limited concurrency + 429 back-off to avoid being rate-limited.
        const int maxConcurrency = 5;
        var semaphore = new SemaphoreSlim(maxConcurrency);
        int resolved = 0;

        var tasks = jobsByUrl.Values.Select(async job =>
        {
            if (job.TraceDeTrailEventUrl is null)
                return (IReadOnlyList<ScrapeJob>)[job];

            await semaphore.WaitAsync(cancellationToken);
            try
            {
                var html = await FetchWithRetryAsync(httpClient, job.TraceDeTrailEventUrl, cancellationToken);
                if (html is null)
                    return (IReadOnlyList<ScrapeJob>)[job];

                var siteUrl = RaceHtmlScraper.ExtractRaceSiteUrl(html, job.TraceDeTrailEventUrl);
                var location = RaceHtmlScraper.ExtractTraceDeTrailLocation(html);

                if (siteUrl is not null)
                    Interlocked.Increment(ref resolved);

                // Try per-course extraction — each course becomes its own ScrapeJob/SourceDiscovery
                var courses = RaceHtmlScraper.ExtractTraceDeTrailCourses(html);
                if (courses.Count > 0)
                {
                    // Map trace ID → ITRA URL so each course links to the right one
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

                    return courses.Select(c =>
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
                    }).ToList();
                }

                // Fallback: single job with event-level values
                var itraPoints = RaceHtmlScraper.ExtractTraceDeTrailItraPoints(html);
                var elevationGain = RaceHtmlScraper.ExtractTraceDeTrailElevationGain(html);
                return (IReadOnlyList<ScrapeJob>)[job with
                {
                    WebsiteUrl = siteUrl ?? job.WebsiteUrl,
                    Location = location ?? job.Location,
                    ItraPoints = itraPoints ?? job.ItraPoints,
                    ElevationGain = elevationGain ?? job.ElevationGain,
                }];
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogDebug(ex, "TraceDeTrail: failed to fetch event page {Url}", job.TraceDeTrailEventUrl);
                return (IReadOnlyList<ScrapeJob>)[job];
            }
            finally
            {
                semaphore.Release();
            }
        }).ToList();

        var enriched = (await Task.WhenAll(tasks)).SelectMany(x => x).ToArray();

        logger.LogInformation("TraceDeTrail: resolved {Resolved}/{Total} event site URLs", resolved, jobsByUrl.Count);
        return enriched;
    }

    /// <summary>
    /// GETs a URL with up to 3 retries on 429 (Too Many Requests), respecting Retry-After.
    /// Returns null on non-success status codes.
    /// </summary>
    private async Task<string?> FetchWithRetryAsync(HttpClient httpClient, Uri url, CancellationToken cancellationToken)
    {
        const int maxRetries = 3;
        for (int attempt = 0; ; attempt++)
        {
            using var response = await httpClient.GetAsync(url, cancellationToken);
            if (response.IsSuccessStatusCode)
                return await response.Content.ReadAsStringAsync(cancellationToken);

            if ((int)response.StatusCode == 429 && attempt < maxRetries)
            {
                var delay = response.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(2 * (attempt + 1));
                logger.LogDebug("TraceDeTrail: 429 on {Url}, retrying in {Delay}s (attempt {Attempt}/{Max})",
                    url, delay.TotalSeconds, attempt + 1, maxRetries);
                await Task.Delay(delay, cancellationToken);
                continue;
            }

            logger.LogDebug("TraceDeTrail: {Status} fetching {Url}", response.StatusCode, url);
            return null;
        }
    }

    // Queries the RunAgain search API (POST https://cloudrun-pgjjiy2k6a-ew.a.run.app/find_runs)
    // for each race type, paging through all results.  Yields one ScrapeJob per discovered event.
    private async Task<IReadOnlyCollection<ScrapeJob>> FetchRunagainJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        var seenUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var jobs = new List<ScrapeJob>();
        var oneYearAgoTimestamp = DateTimeOffset.UtcNow.AddYears(-1).ToUnixTimeSeconds();

        var raceTypeFilter = string.Join(", ", RunagainRaceTypes.Select(t => $"'{t}'"));
        var terrainFilter = string.Join(", ", RunagainTerrainTypes.Select(t => $"'{t}'"));
        var filter = $"(race_type IN [{raceTypeFilter}] OR terrain_type IN [{terrainFilter}]) AND timestamp >= {oneYearAgoTimestamp}";
        const int pageSize = 500;

        for (int offset = 0; ; offset += pageSize)
        {
            string json;
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, RunagainApiUrl)
                {
                    Content = new StringContent(
                        System.Text.Json.JsonSerializer.Serialize(new { search = "", filter, offset, sort = "date", limit = pageSize }),
                        System.Text.Encoding.UTF8,
                        "application/json")
                };
                var response = await httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
                json = await response.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "RunAgain: failed to fetch search results (offset {Offset})", offset);
                break;
            }

            var page = RaceScrapeDiscovery.ParseRunagainSearchResults(json);
            if (page.Count == 0)
                break;

            foreach (var job in page)
            {
                if (job.RunagainUrl is not null && seenUrls.Add(job.RunagainUrl.AbsoluteUri))
                    jobs.Add(job);
            }

            if (page.Count < pageSize)
                break;
        }

        logger.LogInformation("RunAgain: discovered {Count} unique events", jobs.Count);

        // Enrich with organizer website URLs from RunAgain's Firestore (batchGet, 100 docs/request).
        await EnrichRunagainWebsiteUrlsAsync(httpClient, jobs, cancellationToken);

        return jobs;
    }

    // Fetches 'link' (organizer website) and 'registration' fields from RunAgain's
    // public Firestore, using race_guid as the document ID.  Mutates jobs in-place,
    // setting WebsiteUrl to the organizer link when available.
    private async Task EnrichRunagainWebsiteUrlsAsync(HttpClient httpClient, List<ScrapeJob> jobs, CancellationToken cancellationToken)
    {
        // Build guid → index lookup (only for jobs that have a runagain ExternalId and no WebsiteUrl yet)
        var guidToIndices = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        for (int i = 0; i < jobs.Count; i++)
        {
            if (jobs[i].ExternalIds?.TryGetValue("runagain", out var guid) == true && !string.IsNullOrWhiteSpace(guid))
            {
                if (!guidToIndices.TryGetValue(guid, out var list))
                {
                    list = [];
                    guidToIndices[guid] = list;
                }
                list.Add(i);
            }
        }

        if (guidToIndices.Count == 0) return;

        // Batch in groups of 100 (Firestore limit)
        var allGuids = guidToIndices.Keys.ToList();
        int enriched = 0;

        for (int batch = 0; batch < allGuids.Count; batch += 100)
        {
            var batchGuids = allGuids.Skip(batch).Take(100).ToList();
            var documentPaths = batchGuids.Select(g => $"{RunagainFirestoreDocPrefix}{g}").ToList();

            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, RunagainFirestoreBatchUrl)
                {
                    Content = new StringContent(
                        JsonSerializer.Serialize(new
                        {
                            documents = documentPaths,
                            mask = new { fieldPaths = new[] { "link", "registration", "organizer", "more_information", "start_fee", "currency", "county", "date" } }
                        }),
                        System.Text.Encoding.UTF8,
                        "application/json")
                };
                var response = await httpClient.SendAsync(request, cancellationToken);
                response.EnsureSuccessStatusCode();
                var json = await response.Content.ReadAsStringAsync(cancellationToken);

                using var doc = JsonDocument.Parse(json);
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    if (!item.TryGetProperty("found", out var found)) continue;
                    if (!found.TryGetProperty("name", out var nameEl)) continue;

                    var docName = nameEl.GetString() ?? "";
                    var guid = docName.Replace(RunagainFirestoreDocPrefix, "");
                    if (!guidToIndices.TryGetValue(guid, out var indices)) continue;

                    if (!found.TryGetProperty("fields", out var fields)) continue;

                    // Extract link (organizer website) — prefer 'link' over 'registration'
                    string? websiteUrl = null;
                    if (fields.TryGetProperty("link", out var linkEl)
                        && linkEl.TryGetProperty("stringValue", out var linkVal))
                        websiteUrl = linkVal.GetString();

                    if (string.IsNullOrWhiteSpace(websiteUrl)
                        && fields.TryGetProperty("registration", out var regEl)
                        && regEl.TryGetProperty("stringValue", out var regVal))
                        websiteUrl = regVal.GetString();

                    Uri? parsedUrl = null;
                    if (!string.IsNullOrWhiteSpace(websiteUrl)
                        && Uri.TryCreate(websiteUrl, UriKind.Absolute, out var url)
                        && url.Scheme is "http" or "https")
                        parsedUrl = url;

                    // Extract additional Firestore-only fields
                    static string? GetStringField(JsonElement fields, string name) =>
                        fields.TryGetProperty(name, out var el) && el.TryGetProperty("stringValue", out var v)
                            ? v.GetString() : null;

                    var organizer = GetStringField(fields, "organizer");
                    var description = GetStringField(fields, "more_information");
                    var startFee = GetStringField(fields, "start_fee");
                    var currency = GetStringField(fields, "currency");
                    var county = GetStringField(fields, "county");
                    var firestoreDate = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(GetStringField(fields, "date"));

                    foreach (var idx in indices)
                    {
                        var j = jobs[idx];
                        jobs[idx] = j with
                        {
                            WebsiteUrl = parsedUrl ?? j.WebsiteUrl,
                            Organizer = string.IsNullOrWhiteSpace(organizer) ? j.Organizer : organizer,
                            Description = string.IsNullOrWhiteSpace(description) ? j.Description : description,
                            StartFee = string.IsNullOrWhiteSpace(startFee) ? j.StartFee : startFee,
                            Currency = string.IsNullOrWhiteSpace(currency) ? j.Currency : currency,
                            County = string.IsNullOrWhiteSpace(county) ? j.County : county,
                            Date = string.IsNullOrWhiteSpace(j.Date) ? firestoreDate : j.Date,
                        };
                        if (parsedUrl is not null) enriched++;
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "RunAgain: failed to enrich website URLs from Firestore (batch {Batch})", batch / 100);
            }
        }

        logger.LogInformation("RunAgain: enriched {Count}/{Total} events with organizer website URLs", enriched, jobs.Count);
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
