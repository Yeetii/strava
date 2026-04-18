using BAMCIS.GeoJSON;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Geo;
using Shared.Models;
using Shared.Services;

namespace Backend;

/// <summary>
/// Triggered by the assembleRace Service Bus queue (enqueued by <see cref="ScrapeRaceWorker"/>
/// after each scrape run). Reads the <see cref="RaceOrganizerDocument"/>, combines discovery
/// hints and scraper output into final <see cref="StoredFeature"/> race documents, and upserts
/// them into the races Cosmos container.
/// </summary>
public class AssembleRaceWorker(
    RaceOrganizerClient organizerClient,
    RaceCollectionClient raceCollectionClient,
    ILogger<AssembleRaceWorker> logger)
{
    // Discovery source priority (highest → lowest).
    internal static readonly string[] DiscoveryPriority = ["utmb", "tracedetrail", "runagain", "loppkartan"];

    // Scraper key priority (highest → lowest).
    internal static readonly string[] ScraperPriority = ["utmb", "itra", "bfs"];

    [Function(nameof(AssembleRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.AssembleRace, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var organizerKey = message.Body.ToString().Trim();
        if (string.IsNullOrWhiteSpace(organizerKey))
        {
            logger.LogWarning("Empty organizer key (MessageId={MessageId})", message.MessageId);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "EmptyOrganizerKey");
            return;
        }

        var doc = await organizerClient.GetByIdMaybe(
            organizerKey, new PartitionKey(organizerKey), cancellationToken);

        if (doc is null)
        {
            logger.LogWarning("Organizer document not found: {Key}", organizerKey);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "DocumentNotFound",
                deadLetterErrorDescription: $"No document for key '{organizerKey}'");
            return;
        }

        try
        {
            var races = AssembleRaces(doc);
            logger.LogInformation("Assembling {Count} races for {Key}", races.Count, organizerKey);

            foreach (var race in races)
            {
                await raceCollectionClient.UpsertDocument(race, cancellationToken);
            }

            // Record assembly timestamp on the organizer document.
            await organizerClient.PatchLastAssembledAsync(organizerKey, cancellationToken);

            logger.LogInformation("Assembled {Count} races for {Key}", races.Count, organizerKey);
            await TryCompleteAsync(actions, message, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "AssembleRaceWorker failed for {Key} (MessageId={MessageId})",
                organizerKey, message.MessageId);
            await TryDeadLetterAsync(actions, message, organizerKey, ex);
        }
    }

    // ── Settlement helpers ───────────────────────────────────────────────

    private async Task TryCompleteAsync(ServiceBusMessageActions actions, ServiceBusReceivedMessage message, CancellationToken ct)
    {
        try { await actions.CompleteMessageAsync(message, ct); }
        catch (Exception ex) { logger.LogDebug(ex, "Could not complete message (manual trigger?)"); }
    }

    private async Task TryDeadLetterAsync(ServiceBusMessageActions actions, ServiceBusReceivedMessage message, string key, Exception inner)
    {
        try
        {
            await actions.DeadLetterMessageAsync(message,
                deadLetterReason: nameof(AssembleRaceWorker),
                deadLetterErrorDescription: $"{key}: {inner.Message}");
        }
        catch (Exception ex) { logger.LogDebug(ex, "Could not dead-letter message (manual trigger?)"); }
    }

    // ── Assembly ─────────────────────────────────────────────────────────

    /// <summary>
    /// Combines discovery hints and scraper output from <paramref name="doc"/> into a list of
    /// <see cref="StoredFeature"/> race documents ready for the races Cosmos container.
    /// Race IDs are built from the organizer id: <c>{organizerKey}-{index}</c> where index is
    /// assigned after sorting routes by distance (ascending).
    /// </summary>
    public static List<StoredFeature> AssembleRaces(RaceOrganizerDocument doc)
    {
        var organizerKey = doc.Id;

        // 1. Collect merged discovery metadata (priority-ordered per source).
        var mergedDiscovery = MergeDiscovery(doc.Discovery);

        // 2. Collect all scraped routes in priority order (routes with coordinates come first).
        var allRoutes = CollectRoutes(doc.Scrapers);

        var results = new List<StoredFeature>();

        if (allRoutes.Count == 0)
        {
            // No scraper output — fall back to a single point per discovery entry (if coords available).
            var discoveries = doc.Discovery?.Values.SelectMany(x => x).ToList() ?? [];
            int idx = 0;
            foreach (var d in discoveries)
            {
                if (d.Latitude is null || d.Longitude is null)
                    continue;
                var featureId = $"{organizerKey}-{idx++}";
                var props = BuildProperties(mergedDiscovery, scraperKey: null, route: null,
                    scraperImageUrl: null, scraperLogoUrl: null, websiteUrl: doc.Url);
                results.Add(BuildPointFeature(featureId, d.Longitude.Value, d.Latitude.Value, props));
            }
            return results;
        }

        // 3. Sort routes by parsed distance km (ascending) then by name for stable IDs across re-runs.
        var sorted = allRoutes
            .OrderBy(r => ParseDistanceKm(r.Route.Distance) ?? double.MaxValue)
            .ThenBy(r => r.Route.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .ToList();

        // 4. Extract the global website URL and imagery from scrapers (in priority order).
        string? scraperWebsite = GetFirstNonNull(doc.Scrapers, ScraperPriority, s => s.WebsiteUrl);
        string? scraperImageUrl = GetFirstNonNull(doc.Scrapers, ScraperPriority, s => s.ImageUrl);
        string? scraperLogoUrl = GetFirstNonNull(doc.Scrapers, ScraperPriority, s => s.LogoUrl);
        string websiteUrl = scraperWebsite ?? doc.Url;

        // 5. Fetch first discovery entry that has coordinates for point fallback.
        var coordsDiscovery = doc.Discovery?.Values
            .SelectMany(x => x)
            .FirstOrDefault(d => d.Latitude is not null && d.Longitude is not null);

        for (int i = 0; i < sorted.Count; i++)
        {
            var (scraperKey, route) = sorted[i];
            var featureId = $"{organizerKey}-{i}";

            var props = BuildProperties(mergedDiscovery, scraperKey, route, scraperImageUrl, scraperLogoUrl, websiteUrl);

            // Try to build a LineString if the route has coordinates.
            if (route.Coordinates is { Count: >= 2 })
            {
                var positions = route.Coordinates
                    .Where(c => c.Length >= 2)
                    .Select(c => new Position(c[0], c[1]))
                    .ToArray();

                if (positions.Length >= 2)
                {
                    results.Add(BuildLineFeature(featureId, positions, props));
                    continue;
                }
            }

            // Fall back to a point using discovery coordinates.
            if (coordsDiscovery is not null)
            {
                results.Add(BuildPointFeature(featureId,
                    coordsDiscovery.Longitude!.Value, coordsDiscovery.Latitude!.Value, props));
            }
            // Otherwise skip this route (no position can be determined).
        }

        return results;
    }

    // ── Merge helpers ────────────────────────────────────────────────────

    /// <summary>
    /// Merges discovery hints from all sources into a single flattened view.
    /// Sources are ordered by <see cref="DiscoveryPriority"/>; the first non-null value wins.
    /// </summary>
    public static SourceDiscovery MergeDiscovery(Dictionary<string, List<SourceDiscovery>>? discovery)
    {
        if (discovery is null || discovery.Count == 0)
            return new SourceDiscovery { DiscoveredAtUtc = DateTime.UtcNow.ToString("o") };

        // Flatten in priority order: utmb entries first, then tracedetrail, etc.
        var ordered = DiscoveryPriority
            .Where(discovery.ContainsKey)
            .SelectMany(k => discovery[k])
            .Concat(discovery
                .Where(kv => !DiscoveryPriority.Contains(kv.Key))
                .SelectMany(kv => kv.Value))
            .ToList();

        var merged = new SourceDiscovery { DiscoveredAtUtc = DateTime.UtcNow.ToString("o") };
        foreach (var d in ordered)
        {
            merged.Name ??= d.Name;
            merged.Date ??= d.Date;
            merged.Distance ??= d.Distance;
            merged.Country ??= d.Country;
            merged.Location ??= d.Location;
            merged.RaceType ??= d.RaceType;
            merged.ImageUrl ??= d.ImageUrl;
            merged.LogoUrl ??= d.LogoUrl;
            merged.Organizer ??= d.Organizer;
            merged.Description ??= d.Description;
            merged.StartFee ??= d.StartFee;
            merged.Currency ??= d.Currency;
            merged.County ??= d.County;
            merged.TypeLocal ??= d.TypeLocal;
            merged.Latitude ??= d.Latitude;
            merged.Longitude ??= d.Longitude;
            merged.ElevationGain ??= d.ElevationGain;
            merged.RegistrationOpen ??= d.RegistrationOpen;
            merged.Playgrounds ??= d.Playgrounds;
            merged.RunningStones ??= d.RunningStones;
            merged.UtmbWorldSeriesCategory ??= d.UtmbWorldSeriesCategory;
            merged.ItraPoints ??= d.ItraPoints;
            if (d.ExternalIds is { Count: > 0 })
            {
                merged.ExternalIds ??= new Dictionary<string, string>();
                foreach (var (k, v) in d.ExternalIds)
                    merged.ExternalIds.TryAdd(k, v);
            }
            if (d.SourceUrls is { Count: > 0 })
            {
                merged.SourceUrls ??= [];
                foreach (var u in d.SourceUrls)
                    if (!merged.SourceUrls.Contains(u))
                        merged.SourceUrls.Add(u);
            }
        }

        return merged;
    }

    /// <summary>
    /// Collects all scraped routes from all scrapers. Routes that have coordinates are returned
    /// first (highest priority as per the requirement), preserving scraper priority within each
    /// group (utmb → itra → bfs).
    /// </summary>
    public static List<(string ScraperKey, ScrapedRouteOutput Route)> CollectRoutes(
        Dictionary<string, ScraperOutput>? scrapers)
    {
        if (scrapers is null || scrapers.Count == 0)
            return [];

        var withCoords = new List<(string, ScrapedRouteOutput)>();
        var withoutCoords = new List<(string, ScrapedRouteOutput)>();

        // Process known scrapers in priority order.
        foreach (var scraperKey in ScraperPriority)
        {
            if (!scrapers.TryGetValue(scraperKey, out var output) || output.Routes is null)
                continue;

            foreach (var route in output.Routes)
            {
                if (route.Coordinates is { Count: >= 2 })
                    withCoords.Add((scraperKey, route));
                else
                    withoutCoords.Add((scraperKey, route));
            }
        }

        // Also include any scraper not in the priority list (future-proof).
        foreach (var (scraperKey, output) in scrapers)
        {
            if (ScraperPriority.Contains(scraperKey) || output.Routes is null)
                continue;
            foreach (var route in output.Routes)
            {
                if (route.Coordinates is { Count: >= 2 })
                    withCoords.Add((scraperKey, route));
                else
                    withoutCoords.Add((scraperKey, route));
            }
        }

        return [.. withCoords, .. withoutCoords];
    }

    // ── Property building ────────────────────────────────────────────────

    /// <summary>
    /// Builds the properties dictionary for a race feature by merging discovery metadata with
    /// route-specific fields from the scraper output.
    /// </summary>
    public static Dictionary<string, dynamic> BuildProperties(
        SourceDiscovery discovery,
        string? scraperKey,
        ScrapedRouteOutput? route,
        string? scraperImageUrl,
        string? scraperLogoUrl,
        string websiteUrl)
    {
        var props = new Dictionary<string, dynamic>();

        // Name: route name first (more specific), then discovery.
        var name = route?.Name ?? discovery.Name;
        if (!string.IsNullOrWhiteSpace(name))
            props[RaceScrapeDiscovery.PropName] = name;

        // Distance: route distance first, then discovery.
        var distance = route?.Distance ?? discovery.Distance;
        if (!string.IsNullOrWhiteSpace(distance))
            props[RaceScrapeDiscovery.PropDistance] = distance;

        // Elevation gain: route first, then discovery.
        var elevationGain = route?.ElevationGain ?? discovery.ElevationGain;
        if (elevationGain.HasValue)
            props[RaceScrapeDiscovery.PropElevationGain] = elevationGain.Value;

        // Date: prefer newer date; for bfs routes, use the route date when it is newer.
        var date = PickBestDate(scraperKey, route?.Date, discovery.Date);
        if (!string.IsNullOrWhiteSpace(date))
            props[RaceScrapeDiscovery.PropDate] = date;

        // Discovery-sourced metadata.
        if (!string.IsNullOrWhiteSpace(discovery.Country))
            props[RaceScrapeDiscovery.PropCountry] = discovery.Country;

        if (!string.IsNullOrWhiteSpace(discovery.Location))
            props[RaceScrapeDiscovery.PropLocation] = discovery.Location;

        if (!string.IsNullOrWhiteSpace(discovery.RaceType))
            props[RaceScrapeDiscovery.PropRaceType] = discovery.RaceType;

        if (!string.IsNullOrWhiteSpace(discovery.Description))
            props[RaceScrapeDiscovery.PropDescription] = discovery.Description;

        if (!string.IsNullOrWhiteSpace(discovery.Organizer))
            props[RaceScrapeDiscovery.PropOrganizer] = discovery.Organizer;

        // Start fee: route first, then discovery.
        var startFee = route?.StartFee ?? discovery.StartFee;
        if (!string.IsNullOrWhiteSpace(startFee))
            props[RaceScrapeDiscovery.PropStartFee] = startFee;

        var currency = route?.Currency ?? discovery.Currency;
        if (!string.IsNullOrWhiteSpace(currency))
            props[RaceScrapeDiscovery.PropCurrency] = currency;

        // Image / logo: route → scraper-level → discovery.
        var imageUrl = route?.ImageUrl ?? scraperImageUrl ?? discovery.ImageUrl;
        if (!string.IsNullOrWhiteSpace(imageUrl))
            props[RaceScrapeDiscovery.PropImage] = imageUrl;

        var logoUrl = route?.LogoUrl ?? scraperLogoUrl ?? discovery.LogoUrl;
        if (!string.IsNullOrWhiteSpace(logoUrl))
            props[RaceScrapeDiscovery.PropLogo] = logoUrl;

        // Website (canonical organizer URL).
        if (!string.IsNullOrWhiteSpace(websiteUrl))
            props[RaceScrapeDiscovery.PropWebsite] = websiteUrl;

        // UTMB-specific.
        if (discovery.Playgrounds is { Count: > 0 })
            props[RaceScrapeDiscovery.PropPlaygrounds] = discovery.Playgrounds;

        if (discovery.RunningStones.HasValue)
            props[RaceScrapeDiscovery.PropRunningStones] = discovery.RunningStones.Value;

        // Source URLs.
        var sources = BuildSourceUrls(discovery.SourceUrls, route?.SourceUrl, websiteUrl);
        if (sources.Count > 0)
            props[RaceScrapeDiscovery.PropSources] = sources;

        return props;
    }

    /// <summary>
    /// Picks the best date: if the bfs scraper produced a route with a date that is newer than
    /// (or absent from) discovery, use the bfs date; otherwise use the discovery date.
    /// </summary>
    public static string? PickBestDate(string? scraperKey, string? routeDate, string? discoveryDate)
    {
        // Only consider bfs route dates for the "newer date from bfs" preference.
        var candidateRouteDate = scraperKey == "bfs" ? routeDate : null;

        if (string.IsNullOrWhiteSpace(candidateRouteDate))
            return discoveryDate;

        if (string.IsNullOrWhiteSpace(discoveryDate))
            return candidateRouteDate;

        // Use the newer date (lexicographic comparison works for YYYY-MM-DD strings).
        return string.Compare(candidateRouteDate, discoveryDate, StringComparison.Ordinal) > 0
            ? candidateRouteDate
            : discoveryDate;
    }

    private static List<string> BuildSourceUrls(
        List<string>? discoverySourceUrls,
        string? routeSourceUrl,
        string websiteUrl)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();

        void TryAdd(string? url)
        {
            if (string.IsNullOrWhiteSpace(url)) return;
            if (seen.Add(url)) result.Add(url);
        }

        TryAdd(routeSourceUrl);
        if (discoverySourceUrls is not null)
            foreach (var u in discoverySourceUrls)
                TryAdd(u);
        TryAdd(websiteUrl);

        return result;
    }

    // ── StoredFeature helpers ─────────────────────────────────────────────

    private static StoredFeature BuildLineFeature(string featureId, Position[] positions, Dictionary<string, dynamic> props)
    {
        var geometry = new LineString(positions);
        var coord = GeometryCentroidHelper.GetCentroid(geometry);
        var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coord, RaceCollectionClient.DefaultZoom);

        return new StoredFeature
        {
            Id = $"{FeatureKinds.Race}:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.Race,
            Geometry = geometry,
            X = x,
            Y = y,
            Zoom = RaceCollectionClient.DefaultZoom,
            Properties = props,
        };
    }

    private static StoredFeature BuildPointFeature(string featureId, double lng, double lat, Dictionary<string, dynamic> props)
    {
        var geometry = new Point(new Position(lng, lat));
        var coord = new Shared.Models.Coordinate(lng, lat);
        var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coord, RaceCollectionClient.DefaultZoom);

        return new StoredFeature
        {
            Id = $"{FeatureKinds.Race}:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.Race,
            Geometry = geometry,
            X = x,
            Y = y,
            Zoom = RaceCollectionClient.DefaultZoom,
            Properties = props,
        };
    }

    // ── Utility ────────────────────────────────────────────────────────────

    private static T? GetFirstNonNull<T>(
        Dictionary<string, ScraperOutput>? scrapers,
        string[] priority,
        Func<ScraperOutput, T?> selector)
        where T : class
    {
        if (scrapers is null) return null;
        foreach (var key in priority)
        {
            if (scrapers.TryGetValue(key, out var output))
            {
                var value = selector(output);
                if (value is not null) return value;
            }
        }
        return null;
    }

    /// <summary>Parses the first numeric km value from a distance string like "33 km" or "33 km, 21 km".</summary>
    public static double? ParseDistanceKm(string? distance)
    {
        if (string.IsNullOrWhiteSpace(distance))
            return null;

        var first = distance.Split(',')[0].Trim();
        var stripped = System.Text.RegularExpressions.Regex.Replace(first, @"(?i)(km|k|mi|m)\s*$", "").Trim();
        if (double.TryParse(stripped, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var km))
            return km;
        return null;
    }
}
