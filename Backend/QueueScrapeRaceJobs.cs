using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Services;
using System.Net;
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
    private const int MaxTraceDeTrailRequestsPerRun = 450;
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
        // allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("utmb",
        //     await FetchUtmbJobsAsync(httpClient, cancellationToken), cancellationToken));
        //  allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("tracedetrail",
        //      await FetchTraceDeTrailJobsAsync(httpClient, cancellationToken), cancellationToken));
        // allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("runagain",
        //     await FetchRunagainJobsAsync(httpClient, cancellationToken), cancellationToken));
        // allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("loppkartan",
        //     await FetchLoppkartanJobsAsync(httpClient, cancellationToken), cancellationToken));
        allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("itra",
            await FetchItraJobsAsync(httpClient, cancellationToken), cancellationToken));
        // allOrganizerKeys.UnionWith(await DiscoverAndWriteAsync("duv",
        //     await FetchDuvJobsAsync(httpClient, cancellationToken), cancellationToken));

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
        var knownTraceDeTrailIds = await organizerClient.FetchKnownTraceDeTrailIdsAsync(cancellationToken);
        int skippedKnownEvents = 0;
        var requestsRemaining = MaxTraceDeTrailRequestsPerRun;

        foreach (var monthOffset in Enumerable.Range(-12, 25))
        {
            if (requestsRemaining <= 0)
                break;

            var date = now.AddMonths(monthOffset);
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, TraceDeTrailCalendarUrl)
                {
                    Content = new FormUrlEncodedContent(
                    [
                        new KeyValuePair<string, string>("month", date.Month.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                        new KeyValuePair<string, string>("year", date.Year.ToString(System.Globalization.CultureInfo.InvariantCulture))
                    ]),
                };
                request.Headers.Referrer = new Uri(TraceDeTrailCalendarReferer);

                using var response = await httpClient.SendAsync(request, cancellationToken);
                requestsRemaining--;

                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                {
                    logger.LogWarning("TraceDeTrail: calendar returned 429 for {Month}/{Year}, stopping further TraceDeTrail discovery", date.Month, date.Year);
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
                    var eventId = job.ExternalIds is not null &&
                        job.ExternalIds.TryGetValue("tracedetrailEventId", out var tracedetrailEventId)
                        ? tracedetrailEventId
                        : null;
                    var itraEventId = job.ExternalIds is not null &&
                        job.ExternalIds.TryGetValue("itraEventId", out var itraId)
                        ? itraId
                        : null;

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
                logger.LogWarning("TraceDeTrail: received 429 from {Url}, stopping further event page enrichment", job.TraceDeTrailEventUrl);
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

            if (siteUrl is not null)
                resolved++;

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
                        JsonSerializer.Serialize(new { search = "", filter, offset, sort = "date", limit = pageSize }),
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

// ITRA's calendar ignores bbox params — country is the only filter that scopes results.
    // We iterate over every country code so each request stays well under the global ~300 cap.
    private static readonly string[] ItraCountryCodes =
    [
        "AF","AL","DZ","AD","AO","AG","AR","AM","AU","AT","AZ","BS","BH","BD","BB","BY","BE","BZ","BJ","BT",
        "BO","BA","BW","BR","BN","BG","BF","BI","CV","KH","CM","CA","CF","TD","CL","CN","CO","KM","CG","CD",
        "CR","HR","CU","CY","CZ","DK","DJ","DM","DO","EC","EG","SV","GQ","ER","EE","SZ","ET","FJ","FI","FR",
        "GA","GM","GE","DE","GH","GR","GD","GT","GN","GW","GY","HT","HN","HU","IS","IN","ID","IR","IQ","IE",
        "IL","IT","JM","JP","JO","KZ","KE","KI","KW","KG","LA","LV","LB","LS","LR","LY","LI","LT","LU","MG",
        "MW","MY","MV","ML","MT","MH","MR","MU","MX","FM","MD","MC","MN","ME","MA","MZ","MM","NA","NR","NP",
        "NL","NZ","NI","NE","NG","MK","NO","OM","PK","PW","PA","PG","PY","PE","PH","PL","PT","QA","RO","RU",
        "RW","KN","LC","VC","WS","SM","ST","SA","SN","RS","SC","SL","SG","SK","SI","SB","SO","ZA","SS","ES",
        "LK","SD","SR","SE","CH","SY","TW","TJ","TZ","TH","TL","TG","TO","TT","TN","TR","TM","TV","UG","UA",
        "AE","GB","US","UY","UZ","VU","VE","VN","YE","ZM","ZW","RE","GP","MQ","GF","NC","PF","YT","PM","WF",
        "TF","CK","NU","TK","HK","MO","PS","XK","GI","IM","JE","GG","AX","FO","GL","SJ","BM","KY","VG","VI",
        "PR","GU","MP","AS","UM","MF","BL","CW","SX","BQ","AW","AC","TA","IO","SH","FK","GS"
    ];

    // ITRA's calendar is a list view; bounding-box params are ignored.
    // Iterating by country is the only way to retrieve more than the global ~175-race cap.
    private async Task<IReadOnlyCollection<ScrapeJob>> FetchItraJobsAsync(HttpClient ignoredHttpClient, CancellationToken cancellationToken)
    {
        try
        {
            var cookieContainer = new CookieContainer();
            using var handler = new HttpClientHandler
            {
                CookieContainer = cookieContainer,
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };

            using var client = new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(90) };
            client.DefaultRequestHeaders.UserAgent.ParseAdd("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36");
            client.DefaultRequestHeaders.Accept.ParseAdd("text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8");
            client.DefaultRequestHeaders.AcceptLanguage.ParseAdd("en-US,en;q=0.9");
            client.DefaultRequestHeaders.Add("sec-fetch-site", "none");
            client.DefaultRequestHeaders.Add("sec-fetch-mode", "navigate");
            client.DefaultRequestHeaders.Add("sec-fetch-dest", "document");
            client.DefaultRequestHeaders.Add("upgrade-insecure-requests", "1");

            logger.LogInformation("ITRA: fetching calendar page for antiforgery token");
            using var initRequest = new HttpRequestMessage(HttpMethod.Get, ItraDiscoveryAgent.CalendarUrl);
            using var initResponse = await client.SendAsync(initRequest, cancellationToken);
            if (!initResponse.IsSuccessStatusCode)
            {
                logger.LogWarning("ITRA: calendar page returned {Status}", initResponse.StatusCode);
                return [];
            }
            var initialHtml = await initResponse.Content.ReadAsStringAsync(cancellationToken);
            var token = ItraDiscoveryAgent.ExtractRequestVerificationToken(initialHtml);
            if (string.IsNullOrWhiteSpace(token))
            {
                logger.LogWarning("ITRA: failed to extract antiforgery token from calendar page (html length={Length})", initialHtml.Length);
                return [];
            }
            logger.LogInformation("ITRA: got antiforgery token, starting country-by-country discovery");

            var allJobs = new List<ScrapeJob>();
            var seenUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var got429 = false;

            // Run up to 5 country requests concurrently. Token is reused read-only across requests
            // (antiforgery tokens are per-session, not per-request, so sharing is fine).
            await Parallel.ForEachAsync(ItraCountryCodes,
                new ParallelOptions { MaxDegreeOfParallelism = 5, CancellationToken = cancellationToken },
                async (country, ct) =>
            {
                if (got429) return;

                List<KeyValuePair<string, string>> fields =
                [
                    new("__RequestVerificationToken", token),
                    new("Input.Longitude", "0"),
                    new("Input.Latitude", "0"),
                    new("Input.NorthEastLat", "90"),
                    new("Input.NorthEastLng", "180"),
                    new("Input.SouthWestLat", "-90"),
                    new("Input.SouthWestLng", "-180"),
                    new("Input.MinDistance", ""),
                    new("Input.MaxDistance", ""),
                    new("Input.MinElevationGain", ""),
                    new("Input.MaxElevationGain", ""),
                    new("Input.MinElevationLoss", ""),
                    new("Input.MaxElevationLoss", ""),
                    new("Input.MinItraPts", "0"),
                    new("Input.MaxItraPts", "7"),
                    new("Input.ItraPtsValue", "0"),
                    new("Input.NationalLeagues", "false"),
                    new("Input.NationalLeague", "false"),
                    new("Input.DateValue", ""),
                    new("Input.DateStart", ""),
                    new("Input.DateEnd", ""),
                    new("Input.resultcount", "0"),
                    new("Input.RaceValue", ""),
                    new("Input.Country", country),
                    new("ZoomLevel", "2"),
                    new("type", ""),
                    new("countMap", "1"),
                ];

                try
                {
                    using var request = new HttpRequestMessage(HttpMethod.Post, ItraDiscoveryAgent.CalendarUrl)
                    {
                        Content = new FormUrlEncodedContent(fields)
                    };
                    request.Headers.Referrer = ItraDiscoveryAgent.CalendarUrl;

                    using var response = await client.SendAsync(request, ct);
                    if (response.StatusCode == HttpStatusCode.TooManyRequests)
                    {
                        logger.LogWarning("ITRA: received 429 for country {Country}, stopping discovery", country);
                        got429 = true;
                        return;
                    }
                    response.EnsureSuccessStatusCode();

                    var html = await response.Content.ReadAsStringAsync(ct);
                    var page = ItraDiscoveryAgent.ParseCalendarPage(html, ItraDiscoveryAgent.CalendarUrl);

                    int newCount = 0;
                    lock (seenUrls)
                    {
                        foreach (var job in page)
                        {
                            var key = job.WebsiteUrl?.AbsoluteUri ?? job.ItraEventPageUrl?.AbsoluteUri;
                            if (key is null || seenUrls.Add(key))
                            {
                                allJobs.Add(job);
                                newCount++;
                            }
                        }
                    }

                    if (page.Count > 0)
                        logger.LogInformation("ITRA: {Country} → {Count} races ({New} new){Cap}", country, page.Count, newCount,
                            page.Count >= 300 ? " *** MAY BE CAPPED ***" : "");
                }
                catch (Exception ex) when (ex is not OperationCanceledException || ex is TaskCanceledException { CancellationToken.IsCancellationRequested: false })
                {
                    logger.LogWarning(ex, "ITRA: failed to fetch country {Country}", country);
                }
            });

            logger.LogInformation("ITRA: discovered {Count} races before enrichment", allJobs.Count);
            var enrichedJobs = await ItraDiscoveryAgent.EnrichEventPageDetailsAsync(allJobs, client, cancellationToken,
                onProgress: (done, total) =>
                {
                    if (done == -1)
                        logger.LogWarning("ITRA enrichment: bot-blocked (captcha), stopping enrichment");
                    else if (done % 100 == 0)
                        logger.LogInformation("ITRA enrichment: {Done}/{Total}", done, total);
                });
            return enrichedJobs;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "ITRA: failed to initialise calendar session");
            return [];
        }
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

    private async Task<IReadOnlyCollection<ScrapeJob>> FetchDuvJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        const int MaxPages = 10;
        var jobsByUrl = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);
        for (int page = 1; page <= MaxPages; page++)
        {
            var pageUrl = page == 1
                ? DuvDiscoveryAgent.CalendarUrl
                : new UriBuilder(DuvDiscoveryAgent.CalendarUrl) { Query = $"page={page}" }.Uri;

            try
            {
                var html = await httpClient.GetStringAsync(pageUrl, cancellationToken);
                var jobs = DuvDiscoveryAgent.ParseCalendarPage(html, DuvDiscoveryAgent.CalendarUrl);
                if (jobs.Count == 0)
                    break;

                foreach (var job in jobs)
                {
                    if (job.WebsiteUrl is null)
                        continue;

                    jobsByUrl.TryAdd(job.WebsiteUrl.AbsoluteUri, job);
                }

                if (!html.Contains($"calendar.php?&page={page + 1}", StringComparison.OrdinalIgnoreCase))
                    break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "DUV: failed to fetch calendar page {Page}", page);
                break;
            }
        }

        logger.LogInformation("DUV: discovered {Count} calendar events", jobsByUrl.Count);

        var enriched = new List<ScrapeJob>(jobsByUrl.Count);
        foreach (var job in jobsByUrl.Values)
        {
            if (job.WebsiteUrl is null)
            {
                enriched.Add(job);
                continue;
            }

            try
            {
                enriched.Add(await DuvDiscoveryAgent.EnrichJobAsync(httpClient, job, cancellationToken));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "DUV: failed to fetch event detail page for {Url}", job.WebsiteUrl);
                enriched.Add(job);
            }
        }

        return enriched.ToArray();
    }
}
