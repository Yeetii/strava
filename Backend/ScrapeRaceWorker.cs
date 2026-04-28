using Azure.Messaging.ServiceBus;
using Backend.Scrapers;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Constants;

namespace Backend;

/// <summary>
/// Reads an organizer key from the Service Bus queue, fetches the <see cref="RaceOrganizerDocument"/>
/// from Cosmos, runs the scraper pipeline, and writes <see cref="ScraperOutput"/> back to the document.
/// After scraping completes, enqueues the organizer key onto the assembleRace queue so the
/// <see cref="AssembleRaceWorker"/> can produce final <see cref="StoredFeature"/> documents.
/// </summary>
public class ScrapeRaceWorker
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly RaceOrganizerClient _organizerClient;
    private readonly ServiceBusClient _serviceBusClient;
    private readonly ServiceBusSender _assembleSender;
    private readonly ILogger<ScrapeRaceWorker> _logger;

    private readonly IReadOnlyList<(string Key, IRaceScraper Scraper)> _scrapers;
    private readonly BfsScraper _bfsScraper;

    // Domains handled by specialized scrapers or discovery — BFS skips these.
    private static readonly string[] SpecialDomains = ["utmb.world", "itra.run", "tracedetrail.fr", "runagain.com", "statistik.d-u-v.org", "betrail.run"];

    public ScrapeRaceWorker(
        IHttpClientFactory httpClientFactory,
        RaceOrganizerClient organizerClient,
        ServiceBusClient serviceBusClient,
        ILogger<ScrapeRaceWorker> logger)
    {
        _httpClientFactory = httpClientFactory;
        _organizerClient = organizerClient;
        _serviceBusClient = serviceBusClient;
        _assembleSender = serviceBusClient.CreateSender(ServiceBusConfig.AssembleRace);
        _logger = logger;

        _bfsScraper = new BfsScraper(logger);
        _scrapers = [
            ("utmb", new UtmbScraper(logger)),
            ("itra", new ItraScraper(logger)),
        ];
    }

    [Function(nameof(ScrapeRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.ScrapeRace, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var organizerKey = message.Body.ToString().Trim();
        if (string.IsNullOrWhiteSpace(organizerKey))
        {
            _logger.LogWarning("Empty organizer key (MessageId={MessageId})", message.MessageId);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "EmptyOrganizerKey");
            return;
        }

        // 1. Fetch organizer document from Cosmos.
        var doc = await _organizerClient.GetByIdMaybe(
            organizerKey, new Microsoft.Azure.Cosmos.PartitionKey(organizerKey), cancellationToken);

        if (doc is null)
        {
            _logger.LogWarning("Organizer document not found: {Key} (MessageId={MessageId})", organizerKey, message.MessageId);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "DocumentNotFound",
                deadLetterErrorDescription: $"No document for key '{organizerKey}'");
            return;
        }

        // 2. Synthesize a ScrapeJob from the merged discovery data.
        var job = BuildScrapeJobFromDocument(doc);

        // 3. Run scraper pipeline — write output per scraper that returns results.
        var httpClient = _httpClientFactory.CreateClient();
        int scrapersRun = 0;

        try
        {
            // 3a. Specialized scrapers (utmb, itra).
            foreach (var (scraperKey, scraper) in _scrapers)
            {
                if (!scraper.CanHandle(job)) continue;

                RaceScraperResult? result;
                try
                {
                    result = await scraper.ScrapeAsync(job, httpClient, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Scraper {Scraper} failed for {Key}", scraperKey, organizerKey);
                    continue;
                }

                if (result is null) continue;

                var output = ToScraperOutput(result);
                await _organizerClient.WriteScraperOutputAsync(organizerKey, scraperKey, output, cancellationToken);
                scrapersRun++;
                _logger.LogInformation("Scraper/{Scraper}: wrote {RouteCount} routes for {Key}",
                    scraperKey, output.Routes?.Count ?? 0, organizerKey);
            }

            // 3b. BFS scraper — scrape every general URL from the organizer doc.
            var bfsUrls = CollectBfsUrls(doc);
            if (bfsUrls.Count > 0)
            {
                _logger.LogInformation("BFS: scraping {Count} URLs for {Key}", bfsUrls.Count, organizerKey);
                RaceScraperResult? bfsResult;
                try
                {
                    bfsResult = await _bfsScraper.ScrapeAsync(bfsUrls, httpClient, cancellationToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Scraper bfs failed for {Key}", organizerKey);
                    bfsResult = null;
                }

                if (bfsResult is not null)
                {
                    var output = ToScraperOutput(bfsResult);
                    await _organizerClient.WriteScraperOutputAsync(organizerKey, "bfs", output, cancellationToken);
                    scrapersRun++;
                    _logger.LogInformation("Scraper/bfs: wrote {RouteCount} routes for {Key}",
                        output.Routes?.Count ?? 0, organizerKey);
                }
            }

            if (scrapersRun == 0)
                _logger.LogInformation("No scrapers produced output for {Key}", organizerKey);

            // Enqueue the organizer key for assembly — whether or not any scraper ran.
            await _assembleSender.SendMessageAsync(
                new ServiceBusMessage(organizerKey) { ContentType = "text/plain" },
                cancellationToken);

            await TryCompleteAsync(actions, message, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                ex, actions, message, _serviceBusClient, ServiceBusConfig.ScrapeRace, _logger, cancellationToken);
        }
    }

    // ── Settlement helpers ───────────────────────────────────────────────

    private async Task TryCompleteAsync(ServiceBusMessageActions actions, ServiceBusReceivedMessage message, CancellationToken ct)
    {
        try { await actions.CompleteMessageAsync(message, ct); }
        catch (Exception ex) { _logger.LogDebug(ex, "Could not complete message (manual trigger?)"); }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a <see cref="ScrapeJob"/> from all discovery sources in the organizer document,
    /// picking the best (first non-null) value for each field across sources.
    /// </summary>
    internal static ScrapeJob BuildScrapeJobFromDocument(RaceOrganizerDocument doc)
    {
        var allDiscoveries = doc.Discovery?.Values.SelectMany(list => list).ToList() ?? [];

        // Source URLs may contain UTMB, TraceDeTrail ITRA endpoints, TraceDeTrail event pages,
        // RunAgain URLs, generic website URLs, or ITRA source pages.
        Uri? utmbUrl = null;
        var traceDeTrailItraUrls = new List<Uri>();
        Uri? traceDeTrailEventUrl = null;
        Uri? runagainUrl = null;
        Uri? websiteUrl = null;

        foreach (var d in allDiscoveries)
        {
            if (d.SourceUrls is null) continue;
            foreach (var urlStr in d.SourceUrls)
            {
                if (!Uri.TryCreate(urlStr, UriKind.Absolute, out var url)) continue;
                var host = url.Host.ToLowerInvariant();

                if (host.Contains("utmb.world")) utmbUrl ??= url;
                else if (host.Contains("tracedetrail.fr"))
                {
                    if (IsTraceDeTrailItraUrl(url))
                        traceDeTrailItraUrls.Add(url);
                    else if (url.AbsolutePath.Contains("/en/outdoor-trail-running/", StringComparison.OrdinalIgnoreCase)
                          || url.AbsolutePath.Contains("/en/event/", StringComparison.OrdinalIgnoreCase))
                        traceDeTrailEventUrl ??= url;
                    else
                        websiteUrl ??= url;
                }
                else if (host.Contains("runagain.com")) runagainUrl ??= url;
                else websiteUrl ??= url;
            }
        }

        // If no websiteUrl found from source URLs, use the document's canonical URL.
        if (websiteUrl is null && Uri.TryCreate(doc.Url, UriKind.Absolute, out var docUrl))
            websiteUrl = docUrl;

        // Merge discovery fields — first non-null wins.
        string? name = null, date = null, distance = null, country = null, location = null;
        string? raceType = null, imageUrl = null, logoUrl = null, organizer = null, description = null;
        string? startFee = null, currency = null, county = null, typeLocal = null;
        double? lat = null, lng = null, elevationGain = null;
        bool? registrationOpen = null;
        Dictionary<string, string>? externalIds = null;
        List<string>? playgrounds = null;
        int? runningStones = null;
        string? utmbWorldSeriesCategory = null;

        foreach (var d in allDiscoveries)
        {
            name ??= d.Name;
            date ??= d.Date;
            distance ??= d.Distance;
            country ??= d.Country;
            location ??= d.Location;
            raceType ??= d.RaceType;
            imageUrl ??= d.ImageUrl;
            logoUrl ??= d.LogoUrl;
            organizer ??= d.Organizer;
            description ??= d.Description;
            startFee ??= d.StartFee;
            currency ??= d.Currency;
            county ??= d.County;
            typeLocal ??= d.TypeLocal;
            lat ??= d.Latitude;
            lng ??= d.Longitude;
            elevationGain ??= d.ElevationGain;
            registrationOpen ??= d.RegistrationOpen;
            playgrounds ??= d.Playgrounds;
            runningStones ??= d.RunningStones;
            utmbWorldSeriesCategory ??= d.UtmbWorldSeriesCategory;

            if (d.ExternalIds is { Count: > 0 })
            {
                externalIds ??= new(StringComparer.Ordinal);
                foreach (var (k, v) in d.ExternalIds)
                    externalIds.TryAdd(k, v);
            }
        }

        return new ScrapeJob(
            Name: name,
            ExternalIds: externalIds,
            Distance: distance,
            ElevationGain: elevationGain,
            Country: country,
            Location: location,
            RaceType: raceType,
            RegistrationOpen: registrationOpen,
            Date: date,
            ImageUrl: imageUrl,
            LogoUrl: logoUrl,
            Latitude: lat,
            Longitude: lng,
            Playgrounds: playgrounds,
            RunningStones: runningStones,
            UtmbWorldSeriesCategory: utmbWorldSeriesCategory,
            County: county,
            TypeLocal: typeLocal,
            Organizer: organizer,
            Description: description,
            StartFee: startFee,
            Currency: currency,
            UtmbUrl: utmbUrl,
            TraceDeTrailItraUrls: traceDeTrailItraUrls.Count > 0 ? traceDeTrailItraUrls : null,
            TraceDeTrailEventUrl: traceDeTrailEventUrl,
            RunagainUrl: runagainUrl,
            WebsiteUrl: websiteUrl);
    }

    /// <summary>
    /// Collects all general-purpose URLs from the organizer document for BFS scraping.
    /// Includes the canonical URL (from the organizer id/domain) and all source URLs,
    /// excluding domains handled by specialized scrapers or discovery (utmb, itra, tracedetrail, runagain).
    /// </summary>
    internal static IReadOnlyList<Uri> CollectBfsUrls(RaceOrganizerDocument doc)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var urls = new List<Uri>();

        void TryAdd(Uri uri)
        {
            var host = uri.Host.ToLowerInvariant();
            if (SpecialDomains.Any(host.Contains))
                return;
            if (seen.Add(uri.GetLeftPart(UriPartial.Path)))
                urls.Add(uri);
        }

        if (Uri.TryCreate($"https://{doc.Id}", UriKind.Absolute, out var idUrl))
            TryAdd(idUrl);

        if (Uri.TryCreate(doc.Url, UriKind.Absolute, out var docUrl))
            TryAdd(docUrl);

        // All source URLs from all discovery sources.
        if (doc.Discovery is not null)
        {
            foreach (var discoveries in doc.Discovery.Values)
            {
                foreach (var d in discoveries)
                {
                    if (d.SourceUrls is null) continue;
                    foreach (var urlStr in d.SourceUrls)
                    {
                        if (Uri.TryCreate(urlStr, UriKind.Absolute, out var url)
                            && url.Scheme is "http" or "https")
                            TryAdd(url);
                    }
                }
            }
        }

        return urls;
    }

    private static bool IsTraceDeTrailItraUrl(Uri url)
    {
        if (url is null || !url.Host.Contains("tracedetrail.fr", StringComparison.OrdinalIgnoreCase))
            return false;

        var segments = url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        return segments.Length == 3
            && string.Equals(segments[0], "trace", StringComparison.OrdinalIgnoreCase)
            && string.Equals(segments[1], "getTraceItra", StringComparison.OrdinalIgnoreCase)
            && int.TryParse(segments[2], out _);
    }

    /// <summary>
    /// Converts a <see cref="RaceScraperResult"/> into a <see cref="ScraperOutput"/> for storage.
    /// </summary>
    private static ScraperOutput ToScraperOutput(RaceScraperResult result)
    {
        return new ScraperOutput
        {
            ScrapedAtUtc = DateTime.UtcNow.ToString("o"),
            WebsiteUrl = result.WebsiteUrl?.AbsoluteUri,
            ImageUrl = result.ImageUrl?.AbsoluteUri,
            LogoUrl = result.LogoUrl?.AbsoluteUri,
            ExtractedName = result.ExtractedName,
            ExtractedDate = result.ExtractedDate,
            StartFee = result.StartFee,
            Currency = result.Currency,
            Routes = result.Routes.Count > 0
                ? [.. result.Routes.Select(r => new ScrapedRouteOutput
                {
                    Coordinates = r.Coordinates.Count >= 2
                        ? r.Coordinates.Select(c => new[] { c.Lng, c.Lat }).ToList()
                        : null,
                    SourceUrl = r.SourceUrl?.AbsoluteUri,
                    Name = r.Name,
                    Distance = r.Distance,
                    ElevationGain = r.ElevationGain,
                    GpxUrl = r.GpxUrl?.AbsoluteUri,
                    ImageUrl = r.ImageUrl?.AbsoluteUri,
                    LogoUrl = r.LogoUrl?.AbsoluteUri,
                    Date = r.Date,
                    StartFee = r.StartFee,
                    Currency = r.Currency,
                    GpxSource = r.GpxSource,
                })]
                : null,
        };
    }
}
