using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Backend.Scrapers;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Constants;

namespace Backend;

public class ScrapeRaceWorker
{
    private const int Zoom = RaceCollectionClient.DefaultZoom;

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly RaceCollectionClient _racesCollectionClient;
    private readonly ILogger<ScrapeRaceWorker> _logger;

    // Scraper pipeline in priority order:
    // 1. UTMB  2. ITRA (TraceDeTrail direct)  3. TraceDeTrail event page → BFS
    // 4. BFS (Loppkartan / RunAgain organizer website / generic)
    private readonly IReadOnlyList<IRaceScraper> _scrapers;

    public ScrapeRaceWorker(
        IHttpClientFactory httpClientFactory,
        RaceCollectionClient racesCollectionClient,
        ILogger<ScrapeRaceWorker> logger)
    {
        _httpClientFactory = httpClientFactory;
        _racesCollectionClient = racesCollectionClient;
        _logger = logger;

        var bfsScraper = new BfsScraper(logger);
        _scrapers = [
            new UtmbScraper(logger),
            new ItraScraper(logger),
            new TraceDeTrailEventScraper(logger, bfsScraper),
            bfsScraper,
        ];
    }

    [Function(nameof(ScrapeRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.ScrapeRace, Connection = "ServicebusConnection")] ServiceBusReceivedMessage message,
        CancellationToken cancellationToken)
    {
        ScrapeJob? job;
        try
        {
            job = message.Body.ToObjectFromJson<ScrapeJob>();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize ScrapeJob message");
            return;
        }

        if (job is null) return;

        try
        {
            var httpClient = _httpClientFactory.CreateClient();

            // Run all scrapers that can handle this job.
            // Take routes from the highest-priority scraper that returns them,
            // but collect WebsiteUrl from any scraper that provides one.
            RaceScraperResult? routeResult = null;
            Uri? websiteUrl = null;

            foreach (var scraper in _scrapers)
            {
                if (!scraper.CanHandle(job)) continue;

                var result = await scraper.ScrapeAsync(job, httpClient, cancellationToken);
                if (result is null) continue;

                websiteUrl ??= result.WebsiteUrl;

                if (routeResult is null && result.Routes.Count > 0)
                    routeResult = result;
            }

            // Merge the website URL into the route result.
            if (routeResult is not null)
            {
                var merged = routeResult with { WebsiteUrl = websiteUrl ?? routeResult.WebsiteUrl };
                await UpsertRoutesAsync(merged, job, cancellationToken);
                return;
            }

            // All scrapers yielded no routes — fall back to a point feature.
            await UpsertPointFallbackAsync(job, websiteUrl, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "ScrapeRaceWorker: failed to process job (UTMB={Utmb}, ITRA={Itra}, Event={Event}, Website={Web})",
                job.UtmbUrl, job.TraceDeTrailItraUrls, job.TraceDeTrailEventUrl, job.WebsiteUrl);
        }
    }

    // ── Upsert helpers ────────────────────────────────────────────────────────

    private async Task UpsertRoutesAsync(
        RaceScraperResult result,
        ScrapeJob job,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < result.Routes.Count; i++)
        {
            var route = result.Routes[i];
            var websiteUrl = result.WebsiteUrl ?? route.SourceUrl
                ?? job.UtmbUrl ?? job.TraceDeTrailEventUrl ?? job.RunagainUrl ?? job.WebsiteUrl;
            var idUrl = result.WebsiteUrl ?? job.TraceDeTrailEventUrl ?? route.SourceUrl
                ?? job.UtmbUrl ?? job.RunagainUrl ?? job.WebsiteUrl;
            var routeIndex = result.Routes.Count > 1 ? i : (int?)null;

            var featureId = idUrl is not null
                ? RaceScrapeDiscovery.BuildFeatureId(idUrl, routeIndex)
                : RaceScrapeDiscovery.BuildFeatureId(job.Name, route.Distance ?? job.Distance);

            var properties = BuildBaseProperties(job);

            if (websiteUrl is not null)
                properties[RaceScrapeDiscovery.PropWebsite] = websiteUrl.AbsoluteUri;
            if (!string.IsNullOrWhiteSpace(route.Name))
                properties[RaceScrapeDiscovery.PropName] = route.Name;
            if (!string.IsNullOrWhiteSpace(route.Distance))
                properties[RaceScrapeDiscovery.PropDistance] = route.Distance;
            else if (!string.IsNullOrWhiteSpace(job.Distance))
                properties[RaceScrapeDiscovery.PropDistance] = job.Distance;
            if (route.ElevationGain.HasValue)
                properties[RaceScrapeDiscovery.PropElevationGain] = route.ElevationGain.Value;
            if (route.GpxUrl is not null)
                properties["gpxUrl"] = route.GpxUrl.AbsoluteUri;

            var lineString = new LineString(route.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
            var feature = new Feature(lineString, properties, null, new FeatureId(featureId));
            var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
            await _racesCollectionClient.UpsertDocument(stored, cancellationToken);
            _logger.LogInformation("ScrapeRaceWorker: upserted route {FeatureId}", featureId);
        }
    }

    private async Task UpsertPointFallbackAsync(ScrapeJob job, Uri? websiteUrl, CancellationToken cancellationToken)
    {
        if (job.Latitude is null || job.Longitude is null)
        {
            _logger.LogDebug("ScrapeRaceWorker: no routes and no coordinates — skipping job");
            return;
        }

        var sourceUrl = websiteUrl ?? job.WebsiteUrl ?? job.UtmbUrl ?? job.TraceDeTrailEventUrl ?? job.RunagainUrl;
        var featureId = sourceUrl is not null
            ? RaceScrapeDiscovery.BuildFeatureId(sourceUrl)
            : RaceScrapeDiscovery.BuildFeatureId(job.Name, job.Distance);

        if (string.IsNullOrEmpty(featureId))
        {
            _logger.LogWarning("ScrapeRaceWorker: point fallback skipped — could not build feature ID");
            return;
        }

        var properties = BuildBaseProperties(job);
        if (sourceUrl is not null)
            properties[RaceScrapeDiscovery.PropWebsite] = sourceUrl.AbsoluteUri;

        var point = new Point(new Position(job.Longitude.Value, job.Latitude.Value));
        var feature = new Feature(point, properties, null, new FeatureId(featureId));
        var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
        await _racesCollectionClient.UpsertDocument(stored, cancellationToken);
        _logger.LogInformation("ScrapeRaceWorker: upserted point feature {FeatureId}", featureId);
    }

    // Builds the base properties dictionary from a ScrapeJob (used by all upsert paths).
    private static Dictionary<string, dynamic> BuildBaseProperties(ScrapeJob job)
    {
        var baseName = job.Name ?? job.Location ?? "Unnamed";
        var properties = new Dictionary<string, dynamic>
        {
            [RaceScrapeDiscovery.PropName] = baseName,
            [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
        };

        if (!string.IsNullOrWhiteSpace(job.Location))
            properties[RaceScrapeDiscovery.PropLocation] = job.Location;
        if (!string.IsNullOrWhiteSpace(job.County))
            properties["county"] = job.County;
        var normalizedDate = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(job.Date);
        if (!string.IsNullOrWhiteSpace(normalizedDate))
            properties[RaceScrapeDiscovery.PropDate] = normalizedDate;
        var normalizedRaceType = RaceScrapeDiscovery.NormalizeRaceType(job.RaceType);
        if (!string.IsNullOrWhiteSpace(normalizedRaceType))
            properties[RaceScrapeDiscovery.PropRaceType] = normalizedRaceType;
        if (!string.IsNullOrWhiteSpace(job.TypeLocal))
            properties["typeLocal"] = job.TypeLocal;
        var normalizedCountry = RaceScrapeDiscovery.NormalizeCountryToIso2(job.Country);
        if (!string.IsNullOrWhiteSpace(normalizedCountry))
            properties[RaceScrapeDiscovery.PropCountry] = normalizedCountry;
        if (!string.IsNullOrWhiteSpace(job.Distance))
            properties[RaceScrapeDiscovery.PropDistance] = job.Distance;
        if (!string.IsNullOrWhiteSpace(job.ImageUrl))
            properties[RaceScrapeDiscovery.PropImage] = job.ImageUrl;
        if (job.Playgrounds is { Count: > 0 })
            properties[RaceScrapeDiscovery.PropPlaygrounds] = job.Playgrounds;
        if (job.RunningStones > 0)
            properties[RaceScrapeDiscovery.PropRunningStones] = job.RunningStones;
        if (job.ElevationGain.HasValue)
            properties[RaceScrapeDiscovery.PropElevationGain] = job.ElevationGain.Value;
        if (!string.IsNullOrWhiteSpace(job.LogoUrl))
            properties[RaceScrapeDiscovery.PropLogo] = job.LogoUrl;
        if (job.ExternalIds is { Count: > 0 })
            properties["externalIds"] = job.ExternalIds;
        if (!string.IsNullOrWhiteSpace(job.Organizer))
            properties[RaceScrapeDiscovery.PropOrganizer] = job.Organizer;
        if (!string.IsNullOrWhiteSpace(job.Description))
            properties[RaceScrapeDiscovery.PropDescription] = job.Description;
        if (job.RegistrationOpen.HasValue)
            properties["registrationOpen"] = job.RegistrationOpen.Value;
        if (!string.IsNullOrWhiteSpace(job.UtmbWorldSeriesCategory))
            properties["utmbWorldSeriesCategory"] = job.UtmbWorldSeriesCategory;
        if (!string.IsNullOrWhiteSpace(job.StartFee))
        {
            properties[RaceScrapeDiscovery.PropStartFee] = job.StartFee;
            if (!string.IsNullOrWhiteSpace(job.Currency))
                properties[RaceScrapeDiscovery.PropCurrency] = job.Currency;
        }

        var sources = new List<string>();
        if (job.UtmbUrl is not null) sources.Add("utmb");
        if (job.TraceDeTrailItraUrls is { Count: > 0 } || job.TraceDeTrailEventUrl is not null) sources.Add("tracedetrail");
        if (job.RunagainUrl is not null) sources.Add("runagain");
        if (job.WebsiteUrl is not null && job.RunagainUrl is null 
            && job.TraceDeTrailItraUrls is { Count: 0 } && job.TraceDeTrailEventUrl is null) sources.Add("loppkartan");
        if (sources.Count > 0)
            properties[RaceScrapeDiscovery.PropSources] = sources;

        return properties;
    }
}
