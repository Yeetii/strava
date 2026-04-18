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
        [ServiceBusTrigger(ServiceBusConfig.ScrapeRace, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        ScrapeJob? job;
        try
        {
            job = message.Body.ToObjectFromJson<ScrapeJob>();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to deserialize ScrapeJob message (MessageId={MessageId}, DeliveryCount={DeliveryCount})",
                message.MessageId, message.DeliveryCount);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "DeserializationFailed", deadLetterErrorDescription: ex.Message);
            return;
        }

        if (job is null)
        {
            _logger.LogError("ScrapeJob message deserialized to null (MessageId={MessageId}, DeliveryCount={DeliveryCount})",
                message.MessageId, message.DeliveryCount);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "NullScrapeJob", deadLetterErrorDescription: "ScrapeJob message deserialized to null");
            return;
        }

        try
        {
            var httpClient = _httpClientFactory.CreateClient();

            // Run all scrapers that can handle this job.
            // Take routes from the highest-priority scraper that returns them,
            // but collect WebsiteUrl and image/logo from any scraper that provides them.
            RaceScraperResult? routeResult = null;
            Uri? websiteUrl = null;
            Uri? scrapedImageUrl = null;
            Uri? scrapedLogoUrl = null;
            string? scrapedName = null;
            string? scrapedDate = null;

            foreach (var scraper in _scrapers)
            {
                if (!scraper.CanHandle(job)) continue;

                var result = await scraper.ScrapeAsync(job, httpClient, cancellationToken);
                if (result is null) continue;

                websiteUrl ??= result.WebsiteUrl;
                scrapedImageUrl ??= result.ImageUrl;
                scrapedLogoUrl ??= result.LogoUrl;
                scrapedName ??= result.ExtractedName;
                scrapedDate ??= result.ExtractedDate;

                if (routeResult is null && result.Routes.Count > 0)
                    routeResult = result;
            }

            // Merge the website URL into the route result.
            if (routeResult is not null)
            {
                var merged = routeResult with { WebsiteUrl = websiteUrl ?? routeResult.WebsiteUrl };

                // Separate routes that have coordinates (→ LineString) from
                // course-only routes without coordinates (→ Point via fallback).
                var withCoords = merged.Routes.Where(r => r.Coordinates.Count >= 2).ToList();
                var withoutCoords = merged.Routes.Where(r => r.Coordinates.Count < 2).ToList();

                if (withCoords.Count > 0)
                    await UpsertRoutesAsync(merged with { Routes = withCoords }, job, cancellationToken);

                foreach (var course in withoutCoords)
                    await UpsertCoursePointAsync(course, merged.WebsiteUrl, job, cancellationToken);

                await actions.CompleteMessageAsync(message, cancellationToken);
                return;
            }

            // All scrapers yielded no routes — fall back to a point feature.
            await UpsertPointFallbackAsync(job, websiteUrl, scrapedImageUrl, scrapedLogoUrl, scrapedName, scrapedDate, cancellationToken);
            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "ScrapeRaceWorker failed (UTMB={Utmb}, ITRA={Itra}, Event={Event}, Website={Web}, MessageId={MessageId}, DeliveryCount={DeliveryCount})",
                job.UtmbUrl, job.TraceDeTrailItraUrls, job.TraceDeTrailEventUrl, job.WebsiteUrl, message.MessageId, message.DeliveryCount);
            await actions.DeadLetterMessageAsync(message,
                deadLetterReason: nameof(ScrapeRaceWorker),
                deadLetterErrorDescription: $"UTMB={job.UtmbUrl}, ITRA={job.TraceDeTrailItraUrls}, Event={job.TraceDeTrailEventUrl}, Website={job.WebsiteUrl}: {ex.Message}");
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
            if (route.ElevationGain.HasValue && !job.ElevationGain.HasValue)
                properties[RaceScrapeDiscovery.PropElevationGain] = route.ElevationGain.Value;
            if (route.GpxUrl is not null)
                properties["gpxUrl"] = route.GpxUrl.AbsoluteUri;

            // Scraped image/logo/date override discovery-time values.
            if (route.ImageUrl is not null)
                properties[RaceScrapeDiscovery.PropImage] = route.ImageUrl.AbsoluteUri;
            if (route.LogoUrl is not null)
                properties[RaceScrapeDiscovery.PropLogo] = route.LogoUrl.AbsoluteUri;
            if (!string.IsNullOrWhiteSpace(route.Date))
                properties[RaceScrapeDiscovery.PropDate] = route.Date;

            var lineString = new LineString(route.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
            var feature = new Feature(lineString, properties, null, new FeatureId(featureId));
            var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
            await _racesCollectionClient.UpsertDocument(stored, cancellationToken);
            _logger.LogInformation("ScrapeRaceWorker: upserted route {FeatureId}", featureId);
        }
    }

    /// <summary>Upsert a course-page route (no GPX coordinates) as a Point feature.</summary>
    private async Task UpsertCoursePointAsync(
        ScrapedRoute course,
        Uri? resultWebsiteUrl,
        ScrapeJob job,
        CancellationToken cancellationToken)
    {
        if (job.Latitude is null || job.Longitude is null)
        {
            _logger.LogDebug("ScrapeRaceWorker: skipping course point — no job lat/lng");
            return;
        }

        var websiteUrl = resultWebsiteUrl ?? course.SourceUrl
            ?? job.UtmbUrl ?? job.TraceDeTrailEventUrl ?? job.RunagainUrl ?? job.WebsiteUrl;
        var idUrl = resultWebsiteUrl ?? job.TraceDeTrailEventUrl ?? course.SourceUrl
            ?? job.UtmbUrl ?? job.RunagainUrl ?? job.WebsiteUrl;

        var featureId = idUrl is not null
            ? RaceScrapeDiscovery.BuildFeatureId(idUrl)
            : RaceScrapeDiscovery.BuildFeatureId(job.Name, course.Distance ?? job.Distance);

        var properties = BuildBaseProperties(job);

        if (websiteUrl is not null)
            properties[RaceScrapeDiscovery.PropWebsite] = websiteUrl.AbsoluteUri;
        if (!string.IsNullOrWhiteSpace(course.Name))
            properties[RaceScrapeDiscovery.PropName] = course.Name;
        if (!string.IsNullOrWhiteSpace(course.Distance))
            properties[RaceScrapeDiscovery.PropDistance] = course.Distance;
        else if (!string.IsNullOrWhiteSpace(job.Distance))
            properties[RaceScrapeDiscovery.PropDistance] = job.Distance;
        if (course.SourceUrl is not null)
            properties["courseUrl"] = course.SourceUrl.AbsoluteUri;
        if (course.ElevationGain.HasValue && !job.ElevationGain.HasValue)
            properties[RaceScrapeDiscovery.PropElevationGain] = course.ElevationGain.Value;
        if (course.ImageUrl is not null)
            properties[RaceScrapeDiscovery.PropImage] = course.ImageUrl.AbsoluteUri;
        if (course.LogoUrl is not null)
            properties[RaceScrapeDiscovery.PropLogo] = course.LogoUrl.AbsoluteUri;
        if (!string.IsNullOrWhiteSpace(course.Date))
            properties[RaceScrapeDiscovery.PropDate] = course.Date;

        var point = new Point(new Position(job.Longitude.Value, job.Latitude.Value));
        var feature = new Feature(point, properties, null, new FeatureId(featureId));
        var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
        await _racesCollectionClient.UpsertDocument(stored, cancellationToken);
        _logger.LogInformation("ScrapeRaceWorker: upserted course point {FeatureId}", featureId);
    }

    private async Task UpsertPointFallbackAsync(ScrapeJob job, Uri? websiteUrl, Uri? scrapedImageUrl, Uri? scrapedLogoUrl, string? scrapedName, string? scrapedDate, CancellationToken cancellationToken)
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

        // Scraped name overrides the discovery-time name.
        if (scrapedName is not null)
            properties[RaceScrapeDiscovery.PropName] = scrapedName;

        // Scraped date overrides the discovery-time date.
        if (scrapedDate is not null)
            properties[RaceScrapeDiscovery.PropDate] = scrapedDate;

        if (sourceUrl is not null)
            properties[RaceScrapeDiscovery.PropWebsite] = sourceUrl.AbsoluteUri;

        // Scraped image/logo override discovery-time values.
        if (scrapedImageUrl is not null)
            properties[RaceScrapeDiscovery.PropImage] = scrapedImageUrl.AbsoluteUri;
        if (scrapedLogoUrl is not null)
            properties[RaceScrapeDiscovery.PropLogo] = scrapedLogoUrl.AbsoluteUri;

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
