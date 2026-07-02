using System.Globalization;
using System.Text.RegularExpressions;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
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
    public const int DefaultZoom = 8;
    private static readonly bool UseAnsiColors = !Console.IsOutputRedirected && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("NO_COLOR"));

    // Discovery source priority (highest → lowest).
    public static readonly string[] DiscoveryPriority = ["utmb", "duv", "itra", "tracedetrail", "runagain", "mittlopp", "lopplistan", "loppkartan"];

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
        CancellationToken cancellationToken,
        ILogger? logger = null)
    {
        var organizerKey = doc.Id;
        var assemblyDiscovery = PrepareDiscoveryForAssembly(doc.Discovery);

        var mergedDiscovery = MergeDiscovery(assemblyDiscovery);
        var flatDiscoveries = FlattenDiscoveries(assemblyDiscovery);
        flatDiscoveries = DeduplicateDiscoveriesByDistance(flatDiscoveries);

        var allRoutes = CollectRoutes(doc.Scrapers);
        var dedupedRoutes = DeduplicateRoutesByDistance(allRoutes, flatDiscoveries);

        var results = new List<StoredFeature>();
        var assembledLines = new List<LineCoverageCandidate>();

        if (dedupedRoutes.Count == 0)
        {
            var mergedNoRouteDiscoveries = MergeDiscoveriesByName(flatDiscoveries);
            int idx = 0;
            foreach (var (discoverySource, d) in mergedNoRouteDiscoveries)
            {
                var fallback = await ResolveFallbackCoordinatesAsync(d, null, geocodingService, cancellationToken);
                logger?.LogDebug(
                    "Race asm {OrganizerKey} | src={DiscoverySource} | kind=discovery-only | race='{RaceName}' | dCoords={DiscoveryHasCoordinates} | sharedCoords={SharedDiscoveryHasCoordinates} | loc={HasLocation} | outcome={Outcome} | via={Reason} | lat={Latitude} | lng={Longitude}",
                    organizerKey,
                    AccentSource(discoverySource),
                    d.Name ?? "(unnamed)",
                    AccentBool(d.Latitude is not null && d.Longitude is not null),
                    AccentBool(false),
                    AccentBool(!string.IsNullOrWhiteSpace(d.Location)),
                    AccentOutcome(fallback.Coordinates is not null ? "point" : "skip"),
                    AccentReason(fallback.Reason),
                    AccentCoordinate(fallback.Coordinates?.lat),
                    AccentCoordinate(fallback.Coordinates?.lng));

                if (fallback.Coordinates is null)
                    continue;

                var (lat, lng) = fallback.Coordinates.Value;
                var featureId = $"{organizerKey}-{idx++}";
                var props = BuildProperties(d, scraperKey: null, route: null,
                    scraperImageUrl: null, scraperLogoUrl: null, websiteUrl: doc.Url);
                results.Add(BuildPointFeature(featureId, lng, lat, props));
            }
            return results;
        }

        var sorted = dedupedRoutes
            .OrderBy(r => ParseDistanceKm(r.Route.Distance) ?? double.MaxValue)
            .ThenBy(r => r.Route.Name ?? string.Empty, StringComparer.OrdinalIgnoreCase)
            .ToList();

        string? scraperWebsite = GetFirstNonNull(doc.Scrapers, ScraperPriority, s => s.WebsiteUrl);
        string? scraperImageUrl = GetFirstNonNull(doc.Scrapers, ScraperPriority, s => s.ImageUrl);
        string? scraperLogoUrl = GetFirstNonNull(doc.Scrapers, ScraperPriority, s => s.LogoUrl);
        string websiteUrl = scraperWebsite ?? doc.Url;

        var coordsDiscovery = assemblyDiscovery?.Values
            .SelectMany(x => x)
            .FirstOrDefault(d => d.Latitude is not null && d.Longitude is not null);

        var claimedKm = new HashSet<double>();

        for (int i = 0; i < sorted.Count; i++)
        {
            var (scraperKey, route) = sorted[i];
            var featureId = $"{organizerKey}-{i}";
            var routeKm = ParseDistanceKm(route.Distance);

            var (bestDiscoverySource, bestDiscovery) = FindBestDiscoveryForRoute(routeKm, flatDiscoveries, mergedDiscovery);
            var props = BuildProperties(bestDiscovery, scraperKey, route, scraperImageUrl, scraperLogoUrl, websiteUrl);

            var effectiveKm = ParseEffectiveRouteKmForClaiming(routeKm, props);
            if (effectiveKm.HasValue)
                ClaimRoughlyMatchingDiscoveryDistances(effectiveKm.Value, flatDiscoveries, claimedKm);

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
                    var line = BuildLineFeature(featureId, positions, props);
                    results.Add(line);
                    assembledLines.Add(BuildLineCoverageCandidate(line));
                    continue;
                }
            }

            if (effectiveKm.HasValue && AnyLineRoughlyCoversKm(effectiveKm.Value, assembledLines))
            {
                object? coveredDistanceValue = null;
                var coveredDistanceText = props.TryGetValue(PropDistance, out coveredDistanceValue)
                    ? coveredDistanceValue?.ToString()
                    : route.Distance;
                logger?.LogDebug(
                    "Race asm {OrganizerKey} | src={DiscoverySource} | kind=route-fallback | race='{RaceName}' | routeCoords={RouteHasCoordinates} | dCoords={DiscoveryHasCoordinates} | sharedCoords={SharedDiscoveryHasCoordinates} | loc={HasLocation} | outcome={Outcome} | coveredBy='{Distance}'",
                    organizerKey,
                    AccentSource(bestDiscoverySource),
                    bestDiscovery.Name ?? route.Name ?? featureId,
                    AccentBool(route.Coordinates is { Count: >= 2 }),
                    AccentBool(bestDiscovery.Latitude is not null && bestDiscovery.Longitude is not null),
                    AccentBool(coordsDiscovery is not null),
                    AccentBool(!string.IsNullOrWhiteSpace(bestDiscovery.Location)),
                    AccentOutcome("skip-line-covered"),
                    coveredDistanceText);
                continue;
            }

            var fallback = ResolveExistingCoordinates(bestDiscovery, coordsDiscovery);
            logger?.LogDebug(
                "Race asm {OrganizerKey} | src={DiscoverySource} | kind=route-fallback | race='{RaceName}' | routeCoords={RouteHasCoordinates} | dCoords={DiscoveryHasCoordinates} | sharedCoords={SharedDiscoveryHasCoordinates} | loc={HasLocation} | outcome={Outcome} | via={Reason} | lat={Latitude} | lng={Longitude}",
                organizerKey,
                AccentSource(bestDiscoverySource),
                bestDiscovery.Name ?? route.Name ?? featureId,
                AccentBool(route.Coordinates is { Count: >= 2 }),
                AccentBool(bestDiscovery.Latitude is not null && bestDiscovery.Longitude is not null),
                AccentBool(coordsDiscovery is not null),
                AccentBool(!string.IsNullOrWhiteSpace(bestDiscovery.Location)),
                AccentOutcome(fallback.Coordinates is not null ? "point" : "skip"),
                AccentReason(fallback.Reason),
                AccentCoordinate(fallback.Coordinates?.lat),
                AccentCoordinate(fallback.Coordinates?.lng));

            if (fallback.Coordinates is (var lat, var lng))
                results.Add(BuildPointFeature(featureId, lng, lat, props));
        }

        var unclaimed = GetUnclaimedDiscoveryDistances(flatDiscoveries, claimedKm);
        foreach (var (distKm, distLabel) in unclaimed)
        {
            if (distKm.HasValue && AnyLineRoughlyCoversKm(distKm.Value, assembledLines))
                continue;

            var featureId = $"{organizerKey}-{results.Count}";
            var bestDiscMatch = distKm.HasValue
                ? FindBestDiscoveryForRoute(distKm, flatDiscoveries, mergedDiscovery)
                : ("merged", FindDiscoveryForLabel(distLabel, flatDiscoveries, mergedDiscovery));
            var (bestDiscSource, bestDisc) = bestDiscMatch;
            var props = BuildProperties(bestDisc, scraperKey: null, route: null,
                scraperImageUrl, scraperLogoUrl, websiteUrl);
            props[PropDistance] = distLabel;

            var fallback = await ResolveFallbackCoordinatesAsync(
                bestDisc, coordsDiscovery, geocodingService, cancellationToken);
            logger?.LogDebug(
                "Race asm {OrganizerKey} | src={DiscoverySource} | kind=unclaimed-distance | race='{RaceName}' | distance='{Distance}' | dCoords={DiscoveryHasCoordinates} | sharedCoords={SharedDiscoveryHasCoordinates} | loc={HasLocation} | outcome={Outcome} | via={Reason} | lat={Latitude} | lng={Longitude}",
                organizerKey,
                AccentSource(bestDiscSource),
                bestDisc.Name ?? "(unnamed)",
                distLabel,
                AccentBool(bestDisc.Latitude is not null && bestDisc.Longitude is not null),
                AccentBool(coordsDiscovery is not null),
                AccentBool(!string.IsNullOrWhiteSpace(bestDisc.Location)),
                AccentOutcome(fallback.Coordinates is not null ? "point" : "skip"),
                AccentReason(fallback.Reason),
                AccentCoordinate(fallback.Coordinates?.lat),
                AccentCoordinate(fallback.Coordinates?.lng));

            if (fallback.Coordinates is not (var lat, var lng))
                continue;

            results.Add(BuildPointFeature(featureId, lng, lat, props));
        }

        var merged = MergeSameDistanceFeatures(results);
        AppendDistanceToAmbiguousNames(merged);
        return merged;
    }

    private static Dictionary<string, List<SourceDiscovery>>? PrepareDiscoveryForAssembly(
        Dictionary<string, List<SourceDiscovery>>? discovery)
    {
        if (discovery is null || discovery.Count == 0)
            return discovery;

        var hasNonLoppkartanDiscovery = discovery.Keys.Any(key =>
            !string.Equals(key, "loppkartan", StringComparison.OrdinalIgnoreCase));

        if (!hasNonLoppkartanDiscovery)
            return discovery;

        var result = new Dictionary<string, List<SourceDiscovery>>(discovery.Count, StringComparer.OrdinalIgnoreCase);

        foreach (var (source, entries) in discovery)
        {
            if (!string.Equals(source, "loppkartan", StringComparison.OrdinalIgnoreCase))
            {
                result[source] = entries;
                continue;
            }

            result[source] = [.. entries.Select(CloneWithoutLocationCoordinates)];
        }

        return result;
    }

    private static SourceDiscovery CloneWithoutLocationCoordinates(SourceDiscovery entry)
    {
        return new SourceDiscovery
        {
            DiscoveredAtUtc = entry.DiscoveredAtUtc,
            Name = entry.Name,
            Date = entry.Date,
            Latitude = null,
            Longitude = null,
            Distance = entry.Distance,
            ElevationGain = entry.ElevationGain,
            Country = entry.Country,
            Location = null,
            RaceType = entry.RaceType,
            ImageUrl = entry.ImageUrl,
            LogoUrl = entry.LogoUrl,
            Organizer = entry.Organizer,
            Description = entry.Description,
            StartFee = entry.StartFee,
            Currency = entry.Currency,
            County = entry.County,
            TypeLocal = entry.TypeLocal,
            RegistrationOpen = entry.RegistrationOpen,
            ExternalIds = entry.ExternalIds is null ? null : new Dictionary<string, string>(entry.ExternalIds),
            SourceUrls = entry.SourceUrls is null ? null : [.. entry.SourceUrls],
            ItraPoints = entry.ItraPoints,
            ItraNationalLeague = entry.ItraNationalLeague,
            Playgrounds = entry.Playgrounds is null ? null : [.. entry.Playgrounds],
            RunningStones = entry.RunningStones,
            UtmbWorldSeriesCategory = entry.UtmbWorldSeriesCategory,
        };
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
                    if (km.Value > 0)
                        numeric.Add(km.Value);
                    continue;
                }

                if (!labels.Contains(token, StringComparer.OrdinalIgnoreCase))
                    labels.Add(token);
            }
        }

        var canonicalNumeric = BuildCanonicalRouteKmMap(numeric
                .Distinct()
                .OrderBy(x => x)
                .ToList())
            .Values
            .Distinct()
            .OrderBy(x => x)
            .ToList();

        var parts = canonicalNumeric
            .Select(FormatDistanceKm)
            .Concat(labels)
            .ToList();

        return parts.Count == 0 ? null : string.Join(", ", parts);
    }

    private static string FormatDistanceKm(double km)
    {
        var rounded = Math.Round(km);
        if (Math.Abs(km - rounded) < 1e-9)
            return string.Format(CultureInfo.InvariantCulture, "{0:0} km", rounded);

        return string.Format(CultureInfo.InvariantCulture, "{0:0.###} km", km);
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
        var effectiveWebsiteUrl = GetPreferredWebsiteUrl(discovery, route, websiteUrl);

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
        if (!string.IsNullOrWhiteSpace(effectiveWebsiteUrl))
            props[PropWebsite] = effectiveWebsiteUrl;

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
        var sources = BuildSourceUrls(discovery.SourceUrls, route?.SourceUrl, effectiveWebsiteUrl);
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

    private static string GetPreferredWebsiteUrl(SourceDiscovery discovery, ScrapedRouteOutput? route, string fallbackWebsiteUrl)
    {
        if (discovery.SourceUrls is { Count: > 0 })
        {
            var preferred = discovery.SourceUrls.FirstOrDefault(url =>
                !string.IsNullOrWhiteSpace(url)
                && Uri.TryCreate(url, UriKind.Absolute, out var uri)
                && !uri.Host.Contains("mittlopp.se", StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrWhiteSpace(preferred))
                return preferred;
        }

        return fallbackWebsiteUrl;
    }

    // ── StoredFeature helpers ─────────────────────────────────────────────

    private static StoredFeature BuildLineFeature(string featureId, Position[] positions, Dictionary<string, dynamic> props)
    {
        var geometry = new LineString(positions);
        var coord = GeometryCentroidHelper.GetCentroid(geometry);
        var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coord, DefaultZoom);

        return new StoredFeature
        {
            Id = $"{FeatureKinds.Race}:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.Race,
            Geometry = geometry,
            X = x,
            Y = y,
            Zoom = DefaultZoom,
            Properties = props,
        };
    }

    private static StoredFeature BuildPointFeature(string featureId, double lng, double lat, Dictionary<string, dynamic> props)
    {
        var geometry = new Point(new Position(lng, lat));
        var coord = new Coordinate(lng, lat);
        var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(coord, DefaultZoom);

        return new StoredFeature
        {
            Id = $"{FeatureKinds.Race}:{featureId}",
            FeatureId = featureId,
            Kind = FeatureKinds.Race,
            Geometry = geometry,
            X = x,
            Y = y,
            Zoom = DefaultZoom,
            Properties = props,
        };
    }

    /// <summary>
    /// Projects an assembled race feature to the lightweight geometry stored under each
    /// organizer's <c>races/</c> transparency folder. Line geometries are downgraded to a
    /// marker at their centroid while preserving ids, tile coordinates, and properties.
    /// </summary>
    public static StoredFeature CreateTransparencyMarker(StoredFeature race)
    {
        var centroid = GeometryCentroidHelper.GetCentroid(race.Geometry);

        return new StoredFeature
        {
            Id = race.Id,
            FeatureId = race.FeatureId,
            Kind = race.Kind,
            X = race.X,
            Y = race.Y,
            Zoom = race.Zoom,
            Geometry = new Point(new Position(centroid.Lng, centroid.Lat)),
            Properties = new Dictionary<string, dynamic>(race.Properties),
        };
    }

    private readonly record struct FallbackCoordinateResolution((double lat, double lng)? Coordinates, string Reason);

    private static FallbackCoordinateResolution ResolveExistingCoordinates(
        SourceDiscovery discovery,
        SourceDiscovery? coordsDiscovery)
    {
        if (discovery.Latitude is not null && discovery.Longitude is not null)
            return new((discovery.Latitude.Value, discovery.Longitude.Value), "discovery coordinates");

        if (coordsDiscovery is not null)
            return new((coordsDiscovery.Latitude!.Value, coordsDiscovery.Longitude!.Value), "other discovery coordinates");

        return new(null, "no existing coordinates");
    }

    private static async Task<FallbackCoordinateResolution> ResolveFallbackCoordinatesAsync(
        SourceDiscovery discovery,
        SourceDiscovery? coordsDiscovery,
        ILocationGeocodingService? geocodingService,
        CancellationToken cancellationToken)
    {
        if (discovery.Latitude is not null && discovery.Longitude is not null)
            return new((discovery.Latitude.Value, discovery.Longitude.Value), "discovery coordinates");

        if (coordsDiscovery is not null)
            return new((coordsDiscovery.Latitude!.Value, coordsDiscovery.Longitude!.Value), "other discovery coordinates");

        var location = !string.IsNullOrWhiteSpace(discovery.Location)
            ? discovery.Location.Trim()
            : null;
        if (location is null || geocodingService is null)
            return new(null, location is null ? "missing location" : "no geocoding service");

        var coords = await geocodingService.GeocodeAsync(location, discovery.Country, cancellationToken);
        return new(coords, coords is null ? "geocoding miss" : "geocoding");
    }

    private static string AccentSource(string value) => Accent(value, "36");
    private static string AccentReason(string value) => Accent(value, "33");
    private static string AccentOutcome(string value)
        => value switch
        {
            "point" => Accent(value, "32"),
            _ when value.StartsWith("skip", StringComparison.OrdinalIgnoreCase) => Accent(value, "31"),
            _ => Accent(value, "34")
        };
    private static string AccentBool(bool value) => Accent(value ? "yes" : "no", value ? "32" : "31");
    private static string AccentCoordinate(double? value)
        => value.HasValue ? Accent(value.Value.ToString(CultureInfo.InvariantCulture), "35") : Accent("-", "90");

    private static string Accent(string value, string ansiCode)
        => UseAnsiColors ? $"\u001b[{ansiCode}m{value}\u001b[0m" : value;

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

}
