using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Text.Json;
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
public partial class AssembleRaceWorker(
    RaceOrganizerClient organizerClient,
    RaceCollectionClient raceCollectionClient,
    ServiceBusClient serviceBusClient,
    ILocationGeocodingService geocodingService,
    ILogger<AssembleRaceWorker> logger)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    private readonly ILocationGeocodingService _geocodingService = geocodingService;
    /// <summary>
    /// Increment this whenever property-handling or merge logic changes so that stored
    /// <see cref="RaceSlotHashes"/> from previous assembly runs are automatically invalidated.
    /// </summary>
    internal const int AssemblyVersion = 5;

    // Discovery source priority (highest → lowest).
    internal static readonly string[] DiscoveryPriority = ["utmb", "duv", "itra", "tracedetrail", "runagain", "lopplistan", "loppkartan"];

    // Scraper key priority (highest → lowest).
    internal static readonly string[] ScraperPriority = ["utmb", "itra", "mistral", "bfs"];

    // Matches a leading bare-integer prefix such as "10 " in "10 Sätila Trail 85km".
    // The negative lookahead prevents stripping when the number IS a distance unit (e.g. "50 km …").
    private static readonly Regex LeadingNumericPrefix =
        new(@"^\d+\s+(?!(?:km|mi(?:les?)?)(?:\b|$))", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    // Matches a trailing distance token such as " 85km" or " 85 km" at the end of a name.
    private static readonly Regex TrailingDistanceSuffix =
        new(@"\s+\d+(?:[.,]\d+)?\s*(?:km|mi(?:les?)?)\s*$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    // Matches a trailing bare decimal such as " 43.0" or " 43,5" (no unit) at the end of a name.
    // These are numeric artifacts from discovery sources and are always stripped.
    private static readonly Regex TrailingBareDecimalSuffix =
        new(@"\s+\d+[.,]\d+\s*$", RegexOptions.Compiled);

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
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "EmptyOrganizerKey", cancellationToken: cancellationToken);
            return;
        }

        var doc = await organizerClient.GetByIdMaybe(
            organizerKey, new PartitionKey(organizerKey), cancellationToken);

        if (doc is null)
        {
            logger.LogWarning("Organizer document not found: {Key}", organizerKey);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "DocumentNotFound", deadLetterErrorDescription: $"No document for key '{organizerKey}'", cancellationToken: cancellationToken);
            return;
        }

        try
        {
            var races = await AssembleRacesAsync(doc, _geocodingService, cancellationToken);
            logger.LogInformation("Built {Count} race feature(s) for {Key}", races.Count, organizerKey);

            // Write each race to Cosmos, skipping or downgrading to a patch when content is unchanged.
            var newHashes = new Dictionary<string, RaceSlotHashes>(races.Count, StringComparer.Ordinal);
            int upserted = 0, patched = 0, skipped = 0;

            foreach (var race in races)
            {
                var slotKey = race.FeatureId!;
                var newHash = ComputeHashes(race);
                newHashes[slotKey] = newHash;

                if (doc.AssemblyHashes?.TryGetValue(slotKey, out var prevHash) == true
                    && prevHash.AssemblyVersion == AssemblyVersion)
                {
                    if (prevHash.GeometryHash == newHash.GeometryHash &&
                        prevHash.PropertiesHash == newHash.PropertiesHash)
                    {
                        skipped++;
                        continue;
                    }

                    if (prevHash.GeometryHash == newHash.GeometryHash)
                    {
                        // Geometry unchanged — patch only the properties path (~1–10 RU vs full upsert).
                        await raceCollectionClient.PatchRacePropertiesAsync(race.Id, race.Properties, cancellationToken);
                        patched++;
                        continue;
                    }
                }

                await raceCollectionClient.UpsertDocument(race, cancellationToken);
                upserted++;
            }

            logger.LogInformation(
                "Assembly writes for {Key}: {Upserted} upserted, {Patched} props-patched, {Skipped} unchanged/skipped",
                organizerKey, upserted, patched, skipped);

            // Expire superseded slots. Fast path uses the stored previous max to avoid a fan-out query.
            RaceCollectionClient.TryGetHighestRaceSlotIndex(organizerKey, races, out var maxSlot);
            var patchOfDeathIds = maxSlot >= 0
                ? await raceCollectionClient.MarkHigherRaceSlotsExpiredAsync(
                    organizerKey, maxSlot, doc.LastMaxSlotIndex, cancellationToken)
                : [];

            if (patchOfDeathIds.Count > 0)
            {
                const int maxIdsInLog = 40;
                var idPreview = patchOfDeathIds.Count <= maxIdsInLog
                    ? string.Join(", ", patchOfDeathIds)
                    : string.Join(", ", patchOfDeathIds.Take(maxIdsInLog))
                      + $" … (+{patchOfDeathIds.Count - maxIdsInLog} more)";
                logger.LogInformation(
                    "Patch of death (ttl=1): patched {Count} superseded race document(s) for {Key} (slot index > {MaxSlot}). Ids: {Ids}",
                    patchOfDeathIds.Count, organizerKey, maxSlot, idPreview);
            }

            // Expire slots that disappeared from this assembly run (e.g. a Point merged into a
            // LineString shifts or removes a slot below maxSlot, which the range-based expiry
            // above would never reach).
            // Two complementary detection strategies:
            //   1. Diff against previous AssemblyHashes (covers steady-state case).
            //   2. Sweep [0, maxSlot) for holes (covers old organizers with null AssemblyHashes
            //      and any transition where hashes were stored without a now-merged slot).
            var newSlotKeys = newHashes.Keys.ToHashSet(StringComparer.Ordinal);
            var mergedAwayDocIds = new HashSet<string>(StringComparer.Ordinal);

            foreach (var k in (doc.AssemblyHashes ?? []).Keys)
                if (!newSlotKeys.Contains(k))
                    mergedAwayDocIds.Add($"{FeatureKinds.Race}:{k}");

            for (int slot = 0; slot < maxSlot; slot++)
            {
                var slotKey = $"{organizerKey}-{slot}";
                if (!newSlotKeys.Contains(slotKey))
                    mergedAwayDocIds.Add($"{FeatureKinds.Race}:{slotKey}");
            }

            var mergedAwayIds = mergedAwayDocIds.ToList();
            if (mergedAwayIds.Count > 0)
            {
                var mergedPatched = await raceCollectionClient.ExpireSpecificSlotsAsync(mergedAwayIds, cancellationToken);
                if (mergedPatched.Count > 0)
                    logger.LogInformation(
                        "Patch of death (merged slots): expired {Count} merged-away race document(s) for {Key}. Ids: {Ids}",
                        mergedPatched.Count, organizerKey, string.Join(", ", mergedPatched));
            }

            // Record assembly metadata (timestamp, max slot, per-slot hashes) on the organizer document.
            await organizerClient.PatchLastAssembledAsync(
                organizerKey,
                maxSlot >= 0 ? maxSlot : null,
                newHashes,
                cancellationToken);
            await TryCompleteAsync(actions, message, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                ex, actions, message, _serviceBusClient, ServiceBusConfig.AssembleRace, logger, cancellationToken);
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
        => AssembleRacesAsync(doc, null, CancellationToken.None).GetAwaiter().GetResult();

    public static async Task<List<StoredFeature>> AssembleRacesAsync(
        RaceOrganizerDocument doc,
        ILocationGeocodingService? geocodingService,
        CancellationToken cancellationToken)
    {
        var organizerKey = doc.Id;

        // 1. Collect merged discovery metadata (priority-ordered per source) as fallback.
        var mergedDiscovery = MergeDiscovery(doc.Discovery);

        // 1b. Flatten individual discovery entries in priority order for per-route matching.
        var flatDiscoveries = FlattenDiscoveries(doc.Discovery);
        flatDiscoveries = DeduplicateDiscoveriesByDistance(flatDiscoveries);

        // 2. Collect all scraped routes in priority order (routes with coordinates come first).
        var allRoutes = CollectRoutes(doc.Scrapers);
        var dedupedRoutes = DeduplicateRoutesByDistance(allRoutes, flatDiscoveries);

        var results = new List<StoredFeature>();
        var geocodedLocationCache = new Dictionary<string, (double lat, double lng)?>(StringComparer.OrdinalIgnoreCase);

        if (dedupedRoutes.Count == 0)
        {
            // No scraper output — fall back to a single point per deduplicated discovery entry.
            int idx = 0;
            foreach (var (_, d) in flatDiscoveries)
            {
                var coords = d.Latitude is not null && d.Longitude is not null
                    ? (d.Latitude.Value, d.Longitude.Value)
                    : await ResolveFallbackCoordinatesAsync(d, null, geocodingService, geocodedLocationCache, cancellationToken);
                if (coords is null)
                    continue;

                var (lat, lng) = coords.Value;
                var featureId = $"{organizerKey}-{idx++}";
                var props = BuildProperties(mergedDiscovery, scraperKey: null, route: null,
                    scraperImageUrl: null, scraperLogoUrl: null, websiteUrl: doc.Url);
                results.Add(BuildPointFeature(featureId, lng, lat, props));
            }
            return results;
        }

        // 3. Sort routes by parsed distance km (ascending) then by name for stable IDs across re-runs.
        var sorted = dedupedRoutes
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

        // 6. Track which discovery distances (km) are claimed by scraper routes.
        var claimedKm = new HashSet<double>();

        for (int i = 0; i < sorted.Count; i++)
        {
            var (scraperKey, route) = sorted[i];
            var featureId = $"{organizerKey}-{i}";
            var routeKm = ParseDistanceKm(route.Distance);

            // Find the most specific matching discovery entry for this route's distance.
            var bestDiscovery = FindBestDiscoveryForRoute(routeKm, flatDiscoveries, mergedDiscovery);

            var props = BuildProperties(bestDiscovery, scraperKey, route, scraperImageUrl, scraperLogoUrl, websiteUrl);

            // Mark every discovery-listed km that matches the effective route length (route field
            // or merged props) so unclaimed discovery points are not emitted for the same race.
            var effectiveKm = ParseEffectiveRouteKmForClaiming(routeKm, props);
            if (effectiveKm.HasValue)
                ClaimRoughlyMatchingDiscoveryDistances(effectiveKm.Value, flatDiscoveries, claimedKm);

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

            var fallbackCoords = await ResolveFallbackCoordinatesAsync(
                bestDiscovery, coordsDiscovery, geocodingService, geocodedLocationCache, cancellationToken);

            if (fallbackCoords is (var lat, var lng))
            {
                if (effectiveKm.HasValue && AssemblyAlreadyCoversRoughDistanceKm(effectiveKm.Value, results))
                    continue;

                results.Add(BuildPointFeature(featureId, lng, lat, props));
            }
            // Otherwise skip this route (no position can be determined).
        }

        // 7. Add point races for discovery distances not claimed by any scraper route.
        var unclaimed = GetUnclaimedDiscoveryDistances(flatDiscoveries, claimedKm);
        foreach (var (distKm, distLabel) in unclaimed)
        {
            if (distKm.HasValue && AssemblyAlreadyCoversRoughDistanceKm(distKm.Value, results))
                continue;

            var featureId = $"{organizerKey}-{results.Count}";
            var bestDisc = distKm.HasValue
                ? FindBestDiscoveryForRoute(distKm, flatDiscoveries, mergedDiscovery)
                : FindDiscoveryForLabel(distLabel, flatDiscoveries, mergedDiscovery);
            var props = BuildProperties(bestDisc, scraperKey: null, route: null,
                scraperImageUrl, scraperLogoUrl, websiteUrl);
            props[RaceScrapeDiscovery.PropDistance] = distLabel;

            var fallbackCoords = await ResolveFallbackCoordinatesAsync(
                bestDisc, coordsDiscovery, geocodingService, geocodedLocationCache, cancellationToken);
            if (fallbackCoords is not (var lat, var lng))
                continue;

            results.Add(BuildPointFeature(featureId, lng, lat, props));
        }

        var merged = MergeSameDistanceFeatures(results);
        AppendDistanceToAmbiguousNames(merged);
        return merged;
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
    /// Flattens all discovery entries from all sources into a list ordered by
    /// <see cref="DiscoveryPriority"/> (highest first).
    /// </summary>
    internal static List<(string Source, SourceDiscovery Entry)> FlattenDiscoveries(
        Dictionary<string, List<SourceDiscovery>>? discovery)
    {
        if (discovery is null || discovery.Count == 0) return [];

        var result = new List<(string, SourceDiscovery)>();
        foreach (var key in DiscoveryPriority)
        {
            if (discovery.TryGetValue(key, out var entries))
                foreach (var e in entries)
                    result.Add((key, e));
        }
        foreach (var (key, entries) in discovery)
        {
            if (!DiscoveryPriority.Contains(key))
                foreach (var e in entries)
                    result.Add((key, e));
        }
        return result;
    }

    internal static List<(string Source, SourceDiscovery Entry)> DeduplicateDiscoveriesByDistance(
        IReadOnlyList<(string Source, SourceDiscovery Entry)> discoveries)
    {
        if (discoveries.Count == 0) return [];

        var arr = discoveries.ToList();
        var n = arr.Count;
        var parent = new int[n];
        for (int i = 0; i < n; i++) parent[i] = i;

        int Find(int x)
        {
            while (parent[x] != x)
                x = parent[x] = parent[parent[x]];
            return x;
        }

        void Union(int a, int b)
        {
            a = Find(a);
            b = Find(b);
            if (a != b) parent[b] = a;
        }

        static string NameBucketKey(string? name) =>
            NormalizeNameKey(name) ?? "\u0000anon";

        for (int i = 0; i < n; i++)
        {
            for (int j = i + 1; j < n; j++)
            {
                if (!string.Equals(NameBucketKey(arr[i].Entry.Name), NameBucketKey(arr[j].Entry.Name),
                        StringComparison.Ordinal))
                    continue;
                if (DiscoveryDistanceFieldsRoughMatch(arr[i].Entry.Distance, arr[j].Entry.Distance))
                    Union(i, j);
            }
        }

        var components = new Dictionary<int, List<int>>();
        for (int i = 0; i < n; i++)
        {
            var r = Find(i);
            if (!components.TryGetValue(r, out var list))
                components[r] = list = [];
            list.Add(i);
        }

        var result = new List<(string Source, SourceDiscovery Entry)>();
        foreach (var list in components.Values.OrderBy(g => g.Min()))
        {
            var best = list
                .Select(idx => arr[idx])
                .OrderByDescending(d => ParseDate(d.Entry.Date))
                .ThenByDescending(d => ParseDate(d.Entry.DiscoveredAtUtc))
                .First();
            result.Add(best);
        }

        return result;
    }

    private static string? NormalizeNameKey(string? name)
    {
        if (string.IsNullOrWhiteSpace(name)) return null;
        return name.Trim().ToLowerInvariant();
    }

    private static DateTime ParseDate(string? date)
    {
        if (string.IsNullOrWhiteSpace(date))
            return DateTime.MinValue;

        if (DateTime.TryParseExact(date, "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var exact))
        {
            return exact;
        }

        return DateTime.TryParse(date, CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out var parsed)
            ? parsed
            : DateTime.MinValue;
    }

    /// <summary>
    /// Finds the first discovery entry whose Distance field contains the given non-numeric
    /// label (e.g. "Stafett"). Falls back to <paramref name="fallback"/> if no match.
    /// </summary>
    internal static SourceDiscovery FindDiscoveryForLabel(
        string label,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries,
        SourceDiscovery fallback)
    {
        foreach (var (_, entry) in flatDiscoveries)
        {
            if (string.IsNullOrWhiteSpace(entry.Distance)) continue;
            var tokens = entry.Distance.Split(',').Select(t => t.Trim());
            if (tokens.Any(t => t.Equals(label, StringComparison.OrdinalIgnoreCase)))
                return entry;
        }
        return fallback;
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

        // Distance: route distance is more specific (per-race), fall back to discovery.
        var distance = route?.Distance ?? discovery.Distance;
        if (!string.IsNullOrWhiteSpace(distance))
            props[RaceScrapeDiscovery.PropDistance] = distance;

        // Name: discovery is the authoritative source; fall back to route name.
        // Sanitize: strip leading numeric race-number prefixes and any trailing distance label
        // that contradicts the authoritative distance field.
        var name = discovery.Name ?? route?.Name;
        if (!string.IsNullOrWhiteSpace(name))
            props[RaceScrapeDiscovery.PropName] = SanitizeName(name, distance);

        // Elevation gain: discovery first, fall back to route.
        var elevationGain = discovery.ElevationGain ?? route?.ElevationGain;
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
            props[RaceScrapeDiscovery.PropRaceType] = RaceTypeNormalizer.NormalizeRaceType(discovery.RaceType) ?? "";

        if (!string.IsNullOrWhiteSpace(discovery.Description))
            props[RaceScrapeDiscovery.PropDescription] = discovery.Description;

        if (!string.IsNullOrWhiteSpace(discovery.Organizer))
            props[RaceScrapeDiscovery.PropOrganizer] = discovery.Organizer;

        // Start fee: discovery first, then route.
        var startFee = discovery.StartFee ?? route?.StartFee;
        if (!string.IsNullOrWhiteSpace(startFee))
            props[RaceScrapeDiscovery.PropStartFee] = startFee;

        var currency = discovery.Currency ?? route?.Currency;
        if (!string.IsNullOrWhiteSpace(currency))
            props[RaceScrapeDiscovery.PropCurrency] = currency;

        // Image / logo: discovery → scraper route → scraper-level.
        var imageUrl = discovery.ImageUrl ?? route?.ImageUrl ?? scraperImageUrl;
        if (!string.IsNullOrWhiteSpace(imageUrl))
            props[RaceScrapeDiscovery.PropImage] = imageUrl;

        var logoUrl = discovery.LogoUrl ?? route?.LogoUrl ?? scraperLogoUrl;
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

        if (discovery.ItraPoints.HasValue)
            props[RaceScrapeDiscovery.PropItraPoints] = discovery.ItraPoints.Value;

        if (discovery.ItraNationalLeague.HasValue)
            props[RaceScrapeDiscovery.PropItraNationalLeague] = discovery.ItraNationalLeague.Value;

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

    /// <summary>
    /// Sanitizes a race name by applying two transforms in order:
    /// <list type="number">
    ///   <item>Strip a leading bare-integer prefix that is not itself a distance
    ///     (e.g. "10 Sätila Trail 85km" → "Sätila Trail 85km").</item>
    ///   <item>Strip a trailing distance token whose parsed value does <em>not</em> roughly match
    ///     <paramref name="effectiveDistance"/> — i.e. trust the distance field, not the name
    ///     (e.g. "Sätila Trail 85km" with distance "21 km" → "Sätila Trail").
    ///     When name and distance do match the embedded label is left intact.</item>
    /// </list>
    /// Returns the original (trimmed) name when sanitization would produce an empty string.
    /// </summary>
    internal static string SanitizeName(string name, string? effectiveDistance)
    {
        var result = LeadingNumericPrefix.Replace(name.Trim(), string.Empty).Trim();

        // Strip trailing bare decimal numbers unconditionally (e.g. " 43.0").
        // These are numeric artifacts added by discovery sources — never proper name parts.
        var bareMatch = TrailingBareDecimalSuffix.Match(result);
        if (bareMatch.Success)
        {
            var stripped = TrailingBareDecimalSuffix.Replace(result, string.Empty).Trim();
            if (!string.IsNullOrWhiteSpace(stripped))
                result = stripped;
        }

        var suffixMatch = TrailingDistanceSuffix.Match(result);
        if (suffixMatch.Success && !string.IsNullOrWhiteSpace(effectiveDistance))
        {
            var embeddedKm = ParseDistanceKm(suffixMatch.Value.Trim());
            var effectiveKm = ParseDistanceKm(effectiveDistance);
            if (embeddedKm.HasValue && effectiveKm.HasValue
                && !DistancesRoughMatchKm(embeddedKm.Value, effectiveKm.Value))
            {
                var stripped = TrailingDistanceSuffix.Replace(result, string.Empty).Trim();
                if (!string.IsNullOrWhiteSpace(stripped))
                    result = stripped;
            }
        }

        return string.IsNullOrWhiteSpace(result) ? name.Trim() : result;
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
        var coord = new Coordinate(lng, lat);
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

    private static async Task<(double lat, double lng)?> ResolveFallbackCoordinatesAsync(
        SourceDiscovery discovery,
        SourceDiscovery? coordsDiscovery,
        ILocationGeocodingService? geocodingService,
        IDictionary<string, (double lat, double lng)?> geocodedLocationCache,
        CancellationToken cancellationToken)
    {
        if (coordsDiscovery is not null)
            return (coordsDiscovery.Latitude!.Value, coordsDiscovery.Longitude!.Value);

        var location = !string.IsNullOrWhiteSpace(discovery.Location)
            ? discovery.Location.Trim()
            : null;
        if (location is null || geocodingService is null)
            return null;

        var cacheKey = string.Concat(location, "|", discovery.Country?.ToUpperInvariant() ?? string.Empty);
        if (geocodedLocationCache.TryGetValue(cacheKey, out var cached))
            return cached;

        var coords = await geocodingService.GeocodeAsync(location, discovery.Country, cancellationToken);
        geocodedLocationCache[cacheKey] = coords;
        return coords;
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

    // ── Content hashing ───────────────────────────────────────────────────

    /// <summary>
    /// Computes separate MD5 hashes for the properties and geometry of an assembled race.
    /// Used to detect what actually changed between assembly runs so writes can be skipped or
    /// downgraded to a cheap properties-only patch.
    /// </summary>
    internal static RaceSlotHashes ComputeHashes(StoredFeature race)
        => new()
        {
            AssemblyVersion = AssemblyVersion,
            PropertiesHash = ComputeMd5(SerializeProperties(race.Properties)),
            GeometryHash = ComputeMd5(SerializeGeometry(race.Geometry)),
        };

    private static string SerializeProperties(IDictionary<string, dynamic> props)
    {
        // Sort keys so the hash is order-independent.
        var sorted = new SortedDictionary<string, object?>(StringComparer.Ordinal);
        foreach (var (k, v) in props)
            sorted[k] = (object?)v;
        return JsonSerializer.Serialize(sorted);
    }

    private static string SerializeGeometry(Geometry geometry) => geometry switch
    {
        LineString ls => string.Join(";", ls.Coordinates.Select(p => FormattableString.Invariant($"{p.Longitude},{p.Latitude}"))),
        Point pt => FormattableString.Invariant($"{pt.Coordinates.Longitude},{pt.Coordinates.Latitude}"),
        _ => geometry.Type.ToString(),
    };

    private static string ComputeMd5(string input)
    {
        var hash = MD5.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}
