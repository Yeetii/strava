using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using System.Text.Json;

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
    private static readonly Uri RunagainApiUrl = new("https://cloudrun-pgjjiy2k6a-ew.a.run.app/find_runs");
    private static readonly string RunagainFirestoreBatchUrl = "https://firestore.googleapis.com/v1/projects/nestelop-production/databases/(default)/documents:batchGet";
    private static readonly string RunagainFirestoreDocPrefix = "projects/nestelop-production/databases/(default)/documents/RunCollection/";
    private static readonly string[] RunagainRaceTypes = ["Stiløp"];

    private static readonly Uri LoppkartanMarkersUrl = new("https://www.loppkartan.se/markers-se.json");

    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.ScrapeRace);

    [Function(nameof(QueueScrapeRaceJobs))]
    public async Task Run(
        [TimerTrigger("0 0 2 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var all = new List<ScrapeJob>();

        // Fetch all sources (TraceDeTrail is sequential across months; others are single requests).
        // all.AddRange(await FetchUtmbJobsAsync(httpClient, cancellationToken));
        // all.AddRange(await FetchTraceDeTrailJobsAsync(httpClient, cancellationToken));
        all.AddRange(await FetchRunagainJobsAsync(httpClient, cancellationToken));
        // all.AddRange(await FetchLoppkartanJobsAsync(httpClient, cancellationToken));

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

        logger.LogInformation("TraceDeTrail: discovered {Count} unique jobs", jobsByUrl.Count);
        return [.. jobsByUrl.Values];
    }

    // Queries the RunAgain search API (POST https://cloudrun-pgjjiy2k6a-ew.a.run.app/find_runs)
    // for each race type, paging through all results.  Yields one ScrapeJob per discovered event.
    private async Task<IReadOnlyCollection<ScrapeJob>> FetchRunagainJobsAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        var seenUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var jobs = new List<ScrapeJob>();
        var nowTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        foreach (var raceType in RunagainRaceTypes)
        {
            var filter = $"race_type IN ['{raceType}'] AND timestamp >= {nowTimestamp}";
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

                // Check if we've reached the end
                if (page.Count < pageSize)
                    break;
            }
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
                            mask = new { fieldPaths = new[] { "link", "registration", "organizer", "more_information", "start_fee", "currency", "county" } }
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
