using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

/// <summary>
/// Assembles <see cref="StoredFeature"/> race documents from a <see cref="RaceOrganizerDocument"/>.
/// Extracted from the Backend assembly worker so it can be shared with the PmtilesJob
/// (which builds race tiles directly from organizer documents, skipping the races container).
/// </summary>
public static partial class RaceAssembler
{
    /// <summary>
    /// Increment this whenever property-handling or merge logic changes so that stored
    /// <see cref="RaceSlotHashes"/> from previous assembly runs are automatically invalidated.
    /// </summary>
    public const int AssemblyVersion = 5;

    // Discovery source priority (highest → lowest).
    public static readonly string[] DiscoveryPriority = ["utmb", "duv", "itra", "tracedetrail", "runagain", "lopplistan", "loppkartan"];

    // Scraper key priority (highest → lowest).
    public static readonly string[] ScraperPriority = ["utmb", "itra", "mistral", "bfs"];

    // Property name constants — keep in sync with Backend.RaceScrapeDiscovery.Prop*
    internal const string PropName = "name";
    internal const string PropDate = "date";
    internal const string PropWebsite = "website";
    internal const string PropDistance = "distance";
    internal const string PropElevationGain = "elevationGain";
    internal const string PropRaceType = "raceType";
    internal const string PropDescription = "description";
    internal const string PropCountry = "country";
    internal const string PropLocation = "location";
    internal const string PropImage = "image";
    internal const string PropPlaygrounds = "playgrounds";
    internal const string PropRunningStones = "runningStones";
    internal const string PropItraPoints = "itraPoints";
    internal const string PropItraNationalLeague = "itraNationalLeague";
    internal const string PropSources = "sources";
    internal const string PropLogo = "logo";
    internal const string PropOrganizer = "organizer";
    internal const string PropStartFee = "startFee";
    internal const string PropCurrency = "currency";

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

    // ── Assembly ─────────────────────────────────────────────────────────

    /// <summary>
    /// Combines discovery hints and scraper output from <paramref name="doc"/> into a list of
    /// <see cref="StoredFeature"/> race documents ready for the races Cosmos container.
    /// Race IDs are built from the organizer id: <c>{organizerKey}-{index}</c> where index is
    /// assigned after sorting routes by distance (ascending).
    /// </summary>
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
            var mergedNoRouteDiscoveries = MergeDiscoveriesByName(flatDiscoveries);
            int idx = 0;
            foreach (var (_, d) in mergedNoRouteDiscoveries)
            {
                var coords = d.Latitude is not null && d.Longitude is not null
                    ? (d.Latitude.Value, d.Longitude.Value)
                    : await ResolveFallbackCoordinatesAsync(d, null, geocodingService, geocodedLocationCache, cancellationToken);
                if (coords is null)
                    continue;

                var (lat, lng) = coords.Value;
                var featureId = $"{organizerKey}-{idx++}";
                var props = BuildProperties(d, scraperKey: null, route: null,
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
                    .Where(c => c.Length >= 2
                        && c[0] is >= -180 and <= 180
                        && c[1] is >= -90 and <= 90)
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
            props[PropDistance] = distLabel;

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
    public static List<(string Source, SourceDiscovery Entry)> FlattenDiscoveries(
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

    public static List<(string Source, SourceDiscovery Entry)> DeduplicateDiscoveriesByDistance(
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
            result.Add(MergeDiscoveryCluster(list.Select(idx => arr[idx]).ToList()));
        }

        return result;
    }

    private static List<(string Source, SourceDiscovery Entry)> MergeDiscoveriesByName(
        IReadOnlyList<(string Source, SourceDiscovery Entry)> discoveries)
    {
        if (discoveries.Count <= 1)
            return discoveries.ToList();

        var groups = new List<List<(string Source, SourceDiscovery Entry)>>();
        foreach (var discovery in discoveries)
        {
            var key = NormalizeNameKey(discovery.Entry.Name);
            if (key is null)
            {
                groups.Add([discovery]);
                continue;
            }

            var existing = groups.FirstOrDefault(group =>
                string.Equals(NormalizeNameKey(group[0].Entry.Name), key, StringComparison.Ordinal));

            if (existing is null)
                groups.Add([discovery]);
            else
                existing.Add(discovery);
        }

        return groups.Select(group => MergeDiscoveryCluster(group)).ToList();
    }

    private static (string Source, SourceDiscovery Entry) MergeDiscoveryCluster(
        IReadOnlyList<(string Source, SourceDiscovery Entry)> cluster)
    {
        var merged = new SourceDiscovery
        {
            DiscoveredAtUtc = cluster
                .Select(x => x.Entry.DiscoveredAtUtc)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .OrderByDescending(ParseDate)
                .FirstOrDefault() ?? DateTime.UtcNow.ToString("o"),
            Date = cluster
                .Select(x => x.Entry.Date)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .OrderByDescending(ParseDate)
                .FirstOrDefault(),
            Distance = MergeDiscoveryDistances(cluster.Select(x => x.Entry.Distance)),
            Location = PickMostSpecificString(cluster.Select(x => x.Entry.Location)),
        };

        foreach (var (_, entry) in cluster)
        {
            merged.Name ??= entry.Name;
            merged.Country ??= entry.Country;
            merged.RaceType ??= entry.RaceType;
            merged.ImageUrl ??= entry.ImageUrl;
            merged.LogoUrl ??= entry.LogoUrl;
            merged.Organizer ??= entry.Organizer;
            merged.Description ??= entry.Description;
            merged.StartFee ??= entry.StartFee;
            merged.Currency ??= entry.Currency;
            merged.County ??= entry.County;
            merged.TypeLocal ??= entry.TypeLocal;
            merged.Latitude ??= entry.Latitude;
            merged.Longitude ??= entry.Longitude;
            merged.ElevationGain ??= entry.ElevationGain;
            merged.RegistrationOpen ??= entry.RegistrationOpen;
            merged.Playgrounds ??= entry.Playgrounds;
            merged.RunningStones ??= entry.RunningStones;
            merged.UtmbWorldSeriesCategory ??= entry.UtmbWorldSeriesCategory;
            merged.ItraPoints ??= entry.ItraPoints;
            merged.ItraNationalLeague ??= entry.ItraNationalLeague;

            if (entry.ExternalIds is { Count: > 0 })
            {
                merged.ExternalIds ??= new Dictionary<string, string>();
                foreach (var (key, value) in entry.ExternalIds)
                    merged.ExternalIds.TryAdd(key, value);
            }

            if (entry.SourceUrls is not { Count: > 0 })
                continue;

            merged.SourceUrls ??= [];
            foreach (var url in entry.SourceUrls)
                if (!merged.SourceUrls.Contains(url))
                    merged.SourceUrls.Add(url);
        }

        return (cluster[0].Source, merged);
    }

    private static string? MergeDiscoveryDistances(IEnumerable<string?> distances)
    {
        var numeric = new List<double>();
        var labels = new List<string>();

        foreach (var distance in distances)
        {
            if (string.IsNullOrWhiteSpace(distance))
                continue;

            foreach (var token in distance.Split(',').Select(x => x.Trim()).Where(x => x.Length > 0))
            {
                var km = ParseDistanceKm(token);
                if (km.HasValue)
                {
                    if (!numeric.Contains(km.Value))
                        numeric.Add(km.Value);
                    continue;
                }

                if (!labels.Contains(token, StringComparer.OrdinalIgnoreCase))
                    labels.Add(token);
            }
        }

        var parts = numeric
            .OrderBy(x => x)
            .Select(x => string.Format(CultureInfo.InvariantCulture, "{0:G} km", x))
            .Concat(labels)
            .ToList();

        return parts.Count == 0 ? null : string.Join(", ", parts);
    }

    private static string? PickMostSpecificString(IEnumerable<string?> values)
    {
        return values
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x!.Trim())
            .OrderByDescending(x => x.Length)
            .FirstOrDefault();
    }

    internal static string? NormalizeNameKey(string? name)
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
    public static SourceDiscovery FindDiscoveryForLabel(
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
            props[PropDistance] = distance;

        // Name: discovery is the authoritative source; fall back to route name.
        // Sanitize: strip leading numeric race-number prefixes and any trailing distance label
        // that contradicts the authoritative distance field.
        var name = discovery.Name ?? route?.Name;
        if (!string.IsNullOrWhiteSpace(name))
            props[PropName] = SanitizeName(name, distance);

        // Elevation gain: discovery first, fall back to route.
        var elevationGain = discovery.ElevationGain ?? route?.ElevationGain;
        if (elevationGain.HasValue)
            props[PropElevationGain] = elevationGain.Value;

        // Date: prefer newer date; for bfs routes, use the route date when it is newer.
        var date = PickBestDate(scraperKey, route?.Date, discovery.Date);
        if (!string.IsNullOrWhiteSpace(date))
            props[PropDate] = date;

        // Discovery-sourced metadata.
        if (!string.IsNullOrWhiteSpace(discovery.Country))
            props[PropCountry] = discovery.Country;

        if (!string.IsNullOrWhiteSpace(discovery.Location))
            props[PropLocation] = discovery.Location;

        if (!string.IsNullOrWhiteSpace(discovery.RaceType))
            props[PropRaceType] = RaceTypeNormalizer.NormalizeRaceType(discovery.RaceType) ?? "";

        if (!string.IsNullOrWhiteSpace(discovery.Description))
            props[PropDescription] = discovery.Description;

        if (!string.IsNullOrWhiteSpace(discovery.Organizer))
            props[PropOrganizer] = discovery.Organizer;

        // Start fee: discovery first, then route.
        var startFee = discovery.StartFee ?? route?.StartFee;
        if (!string.IsNullOrWhiteSpace(startFee))
            props[PropStartFee] = startFee;

        var currency = discovery.Currency ?? route?.Currency;
        if (!string.IsNullOrWhiteSpace(currency))
            props[PropCurrency] = currency;

        // Image / logo: discovery → scraper route → scraper-level.
        var imageUrl = discovery.ImageUrl ?? route?.ImageUrl ?? scraperImageUrl;
        if (!string.IsNullOrWhiteSpace(imageUrl))
            props[PropImage] = imageUrl;

        var logoUrl = discovery.LogoUrl ?? route?.LogoUrl ?? scraperLogoUrl;
        if (!string.IsNullOrWhiteSpace(logoUrl))
            props[PropLogo] = logoUrl;

        // Website (canonical organizer URL).
        if (!string.IsNullOrWhiteSpace(websiteUrl))
            props[PropWebsite] = websiteUrl;

        // UTMB-specific.
        if (discovery.Playgrounds is { Count: > 0 })
            props[PropPlaygrounds] = discovery.Playgrounds;

        if (discovery.RunningStones.HasValue)
            props[PropRunningStones] = discovery.RunningStones.Value;

        if (discovery.ItraPoints.HasValue)
            props[PropItraPoints] = discovery.ItraPoints.Value;

        if (discovery.ItraNationalLeague.HasValue)
            props[PropItraNationalLeague] = discovery.ItraNationalLeague.Value;

        // Source URLs.
        var sources = BuildSourceUrls(discovery.SourceUrls, route?.SourceUrl, websiteUrl);
        if (sources.Count > 0)
            props[PropSources] = sources;

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
    ///   <item>Strip a leading bare-integer prefix that is not itself a distance.</item>
    ///   <item>Strip a trailing distance token whose parsed value does not roughly match
    ///     <paramref name="effectiveDistance"/>.</item>
    /// </list>
    /// Returns the original (trimmed) name when sanitization would produce an empty string.
    /// </summary>
    public static string SanitizeName(string name, string? effectiveDistance)
    {
        var result = LeadingNumericPrefix.Replace(name.Trim(), string.Empty).Trim();

        // Strip trailing bare decimal numbers unconditionally (e.g. " 43.0").
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
    public static RaceSlotHashes ComputeHashes(StoredFeature race)
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
