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
    ServiceBusClient serviceBusClient,
    ILogger<AssembleRaceWorker> logger)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    // Discovery source priority (highest → lowest).
    internal static readonly string[] DiscoveryPriority = ["utmb", "duv", "itra", "tracedetrail", "runagain", "loppkartan"];

    // Scraper key priority (highest → lowest).
    internal static readonly string[] ScraperPriority = ["utmb", "itra", "mistral", "bfs"];

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

        if (dedupedRoutes.Count == 0)
        {
            // No scraper output — fall back to a single point per deduplicated discovery entry (if coords available).
            int idx = 0;
            foreach (var (_, d) in flatDiscoveries)
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

            // Claim the closest discovery distance.
            if (routeKm.HasValue)
                ClaimClosestDistance(routeKm.Value, flatDiscoveries, claimedKm);

            var props = BuildProperties(bestDiscovery, scraperKey, route, scraperImageUrl, scraperLogoUrl, websiteUrl);

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

        // 7. Add point races for discovery distances not claimed by any scraper route.
        if (coordsDiscovery is not null)
        {
            var unclaimed = GetUnclaimedDiscoveryDistances(flatDiscoveries, claimedKm);
            foreach (var (distKm, distLabel) in unclaimed)
            {
                var featureId = $"{organizerKey}-{results.Count}";
                var bestDisc = distKm.HasValue
                    ? FindBestDiscoveryForRoute(distKm, flatDiscoveries, mergedDiscovery)
                    : FindDiscoveryForLabel(distLabel, flatDiscoveries, mergedDiscovery);
                var props = BuildProperties(bestDisc, scraperKey: null, route: null,
                    scraperImageUrl, scraperLogoUrl, websiteUrl);
                props[RaceScrapeDiscovery.PropDistance] = distLabel;
                results.Add(BuildPointFeature(featureId,
                    coordsDiscovery.Longitude!.Value, coordsDiscovery.Latitude!.Value, props));
            }
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

    /// <summary>
    /// True when two distances (km) differ by strictly less than 3% of the larger value
    /// (e.g. 509 km vs 511 km).
    /// </summary>
    public static bool DistancesRoughMatchKm(double aKm, double bKm)
    {
        var diff = Math.Abs(aKm - bKm);
        if (diff == 0) return true;
        var longer = Math.Max(aKm, bKm);
        if (longer <= 0) return false;
        return diff < 0.03 * longer;
    }

    private static bool IsPureNumericCommaSeparatedDistance(string distance)
    {
        var tokens = distance.Split(',')
            .Select(t => t.Trim())
            .Where(t => !string.IsNullOrEmpty(t))
            .ToList();
        if (tokens.Count == 0) return false;
        foreach (var token in tokens)
        {
            var stripped = System.Text.RegularExpressions.Regex.Replace(
                token, @"(?i)\s*(km|k|mi|m)\s*$", "").Trim();
            if (!double.TryParse(stripped, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out _))
                return false;
        }
        return true;
    }

    private static bool DiscoveryDistanceFieldsRoughMatch(string? da, string? db)
    {
        if (string.IsNullOrWhiteSpace(da) && string.IsNullOrWhiteSpace(db)) return true;
        if (string.IsNullOrWhiteSpace(da) || string.IsNullOrWhiteSpace(db)) return false;
        if (!IsPureNumericCommaSeparatedDistance(da) || !IsPureNumericCommaSeparatedDistance(db))
            return string.Equals(NormalizeDistanceKey(da), NormalizeDistanceKey(db), StringComparison.Ordinal);

        var na = ParseDistanceList(da).OrderBy(x => x).ToList();
        var nb = ParseDistanceList(db).OrderBy(x => x).ToList();
        if (na.Count != nb.Count) return false;
        for (int i = 0; i < na.Count; i++)
        {
            if (!DistancesRoughMatchKm(na[i], nb[i]))
                return false;
        }
        return true;
    }

    private static string? NormalizeDistanceKey(string? distance)
    {
        if (string.IsNullOrWhiteSpace(distance)) return null;

        var tokens = distance.Split(',')
            .Select(t => t.Trim())
            .Where(t => !string.IsNullOrEmpty(t))
            .ToList();

        if (tokens.Count == 0) return null;

        var numericValues = new List<double>();
        var otherTokens = new List<string>();

        foreach (var token in tokens)
        {
            var stripped = System.Text.RegularExpressions.Regex.Replace(
                token, @"(?i)\s*(km|k|mi|m)\s*$", "").Trim();
            if (double.TryParse(stripped, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                numericValues.Add(value);
            }
            else
            {
                otherTokens.Add(token.ToLowerInvariant());
            }
        }

        if (otherTokens.Count == 0)
            return string.Join(",", numericValues.OrderBy(v => v)
                .Select(v => v.ToString("G", System.Globalization.CultureInfo.InvariantCulture)));

        return string.Join(",", tokens.Select(t => t.ToLowerInvariant()));
    }

    private static DateTime ParseDate(string? date)
    {
        if (string.IsNullOrWhiteSpace(date))
            return DateTime.MinValue;

        if (DateTime.TryParseExact(date, "yyyy-MM-dd",
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.None, out var exact))
        {
            return exact;
        }

        return DateTime.TryParse(date, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
            out var parsed)
            ? parsed
            : DateTime.MinValue;
    }

    /// <summary>
    /// Finds the best matching discovery entry for a route at <paramref name="routeKm"/>.
    /// Prefers entries whose Distance field contains exactly one distance matching the route
    /// (most specific). Falls back to multi-distance entries that contain the distance, then
    /// to <paramref name="fallback"/>.
    /// Within the same specificity tier, the first match by priority order wins.
    /// </summary>
    public static SourceDiscovery FindBestDiscoveryForRoute(
        double? routeKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> orderedDiscoveries,
        SourceDiscovery fallback)
    {
        if (routeKm is null || orderedDiscoveries.Count == 0)
            return fallback;

        // Within ~3% of the longer distance, prefer closest distance match. Single-distance entries win over
        // multi-distance entries at the same delta. Source priority (list order) breaks ties.
        SourceDiscovery? bestSingle = null;
        double bestSingleDelta = double.MaxValue;
        SourceDiscovery? bestMulti = null;
        double bestMultiDelta = double.MaxValue;

        foreach (var (_, entry) in orderedDiscoveries)
        {
            var dists = ParseDistanceList(entry.Distance);
            if (dists.Count == 0) continue;

            var matching = dists.Where(d => DistancesRoughMatchKm(d, routeKm.Value)).ToList();
            if (matching.Count == 0) continue;

            double closestDelta = matching.Min(d => Math.Abs(d - routeKm.Value));

            if (dists.Count == 1)
            {
                if (closestDelta < bestSingleDelta)
                {
                    bestSingle = entry;
                    bestSingleDelta = closestDelta;
                }
            }
            else
            {
                if (closestDelta < bestMultiDelta)
                {
                    bestMulti = entry;
                    bestMultiDelta = closestDelta;
                }
            }
        }

        return bestSingle ?? bestMulti ?? fallback;
    }

    /// <summary>
    /// Parses a comma-separated distance string (e.g. "50 km, 33 km, 21 km") into a list of
    /// numeric km values.
    /// </summary>
    public static List<double> ParseDistanceList(string? distance)
    {
        if (string.IsNullOrWhiteSpace(distance)) return [];

        var result = new List<double>();
        foreach (var token in distance.Split(','))
        {
            var stripped = System.Text.RegularExpressions.Regex.Replace(
                token.Trim(), @"(?i)\s*(km|k)\s*$", "").Trim();
            if (double.TryParse(stripped, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var km))
                result.Add(km);
        }
        return result;
    }

    /// <summary>
    /// Claims the closest discovery distance for the given route km when that discovery distance
    /// is within ~3% of the longer of the two values; the claimed numeric is then added to
    /// <paramref name="claimedKm"/>.
    /// </summary>
    internal static void ClaimClosestDistance(
        double routeKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries,
        HashSet<double> claimedKm)
    {
        double? bestDist = null;
        double bestDelta = double.MaxValue;

        foreach (var (_, entry) in flatDiscoveries)
        {
            foreach (var d in ParseDistanceList(entry.Distance))
            {
                if (!DistancesRoughMatchKm(d, routeKm)) continue;
                var delta = Math.Abs(d - routeKm);
                if (delta < bestDelta)
                {
                    bestDelta = delta;
                    bestDist = d;
                }
            }
        }

        if (bestDist.HasValue)
            claimedKm.Add(bestDist.Value);
    }

    /// <summary>
    /// Returns all individual discovery distances not yet claimed, sorted ascending.
    /// Each item carries the original label (e.g. "21 km") for property use.
    /// </summary>
    internal static List<(double? Km, string Label)> GetUnclaimedDiscoveryDistances(
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries,
        HashSet<double> claimedKm)
    {
        var seenKm = new HashSet<double>();
        var seenLabels = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var numeric = new List<(double? Km, string Label)>();
        var labels = new List<(double? Km, string Label)>();

        foreach (var (_, entry) in flatDiscoveries)
        {
            if (string.IsNullOrWhiteSpace(entry.Distance)) continue;
            foreach (var token in entry.Distance.Split(','))
            {
                var trimmed = token.Trim();
                var stripped = System.Text.RegularExpressions.Regex.Replace(
                    trimmed, @"(?i)\s*(km|k)\s*$", "").Trim();
                if (double.TryParse(stripped, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var km))
                {
                    if (seenKm.Any(s => DistancesRoughMatchKm(s, km))) continue;
                    seenKm.Add(km);
                    if (!claimedKm.Any(c => DistancesRoughMatchKm(c, km)))
                        numeric.Add((km, trimmed));
                }
                else
                {
                    if (seenLabels.Add(trimmed))
                        labels.Add((null, trimmed));
                }
            }
        }

        numeric.Sort((a, b) => a.Km!.Value.CompareTo(b.Km!.Value));
        numeric.AddRange(labels);
        return numeric;
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

    private static List<(string ScraperKey, ScrapedRouteOutput Route)> DeduplicateRoutesByDistance(
        List<(string ScraperKey, ScrapedRouteOutput Route)> routes,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries)
    {
        if (routes.Count <= 1)
            return routes;

        var distinctKm = routes
            .Select(r => ParseDistanceKm(r.Route.Distance))
            .Where(k => k.HasValue)
            .Select(k => k!.Value)
            .Distinct()
            .OrderBy(x => x)
            .ToList();
        var canonicalRouteKm = BuildCanonicalRouteKmMap(distinctKm);

        var grouped = routes
            .GroupBy(r =>
            {
                var km = ParseDistanceKm(r.Route.Distance);
                if (km is null) return (double?)null;
                return canonicalRouteKm.GetValueOrDefault(km.Value, km.Value);
            })
            .ToList();

        var result = new List<(string ScraperKey, ScrapedRouteOutput Route)>();
        foreach (var group in grouped)
        {
            if (group.Key is null)
            {
                result.AddRange(group);
                continue;
            }

            var allowedCount = Math.Max(1, CountMatchingDiscoveryEntries(group.Key.Value, flatDiscoveries));
            var kept = group
                .OrderByDescending(r => r.Route.Date ?? string.Empty)
                .ThenByDescending(r => r.Route.Coordinates is { Count: >= 2 })
                .Take(allowedCount);
            result.AddRange(kept);
        }

        return result;
    }

    /// <summary>
    /// Maps each distinct route km to a canonical km (cluster minimum). Values are clustered in
    /// ascending order; <paramref name="sortedDistinctKm"/> must be sorted ascending.
    /// A value joins the current cluster only if it is within ~3% of the <em>cluster minimum</em>
    /// (same rule as <see cref="DistancesRoughMatchKm"/>), so every pair in a cluster is within
    /// ~3% of <c>max(pair)</c> — not only consecutive neighbors.
    /// </summary>
    internal static Dictionary<double, double> BuildCanonicalRouteKmMap(List<double> sortedDistinctKm)
    {
        var map = new Dictionary<double, double>();
        if (sortedDistinctKm.Count == 0) return map;

        double clusterMin = sortedDistinctKm[0];
        var clusters = new List<List<double>> { new() { clusterMin } };
        foreach (var v in sortedDistinctKm.Skip(1))
        {
            var last = clusters[^1];
            if (DistancesRoughMatchKm(clusterMin, v))
                last.Add(v);
            else
            {
                clusters.Add(new List<double> { v });
                clusterMin = v;
            }
        }

        foreach (var c in clusters)
        {
            var canon = c.Min();
            foreach (var v in c)
                map[v] = canon;
        }

        return map;
    }

    private static int CountMatchingDiscoveryEntries(
        double routeKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries)
    {
        var seenEntries = new HashSet<SourceDiscovery>();

        foreach (var (_, entry) in flatDiscoveries)
        {
            var dists = ParseDistanceList(entry.Distance);
            if (dists.Any(d => DistancesRoughMatchKm(d, routeKm)))
                seenEntries.Add(entry);
        }

        return seenEntries.Count;
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

        // Name: discovery is the authoritative source; fall back to route name.
        var name = discovery.Name ?? route?.Name;
        if (!string.IsNullOrWhiteSpace(name))
            props[RaceScrapeDiscovery.PropName] = name;

        // Distance: route distance is more specific (per-race), fall back to discovery.
        var distance = route?.Distance ?? discovery.Distance;
        if (!string.IsNullOrWhiteSpace(distance))
            props[RaceScrapeDiscovery.PropDistance] = distance;

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
            props[RaceScrapeDiscovery.PropRaceType] = discovery.RaceType;

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
