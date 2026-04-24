using System.Globalization;
using System.Text.RegularExpressions;
using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Backend;

/// <summary>
/// Distance parsing, ~3% rough matching, discovery claiming, route deduplication, and GPX line
/// coverage helpers for <see cref="AssembleRaceWorker"/>.
/// </summary>
partial class AssembleRaceWorker
{
    /// <summary>Trailing unit when validating purely numeric multi-distance fields.</summary>
    private static readonly Regex NumericMultiTokenSuffix = new(@"(?i)\s*(km|k|mi|m)\s*$", RegexOptions.Compiled);

    private static readonly NumberStyles FloatKm = NumberStyles.Float;
    private static readonly CultureInfo Inv = CultureInfo.InvariantCulture;

    /// <summary>
    /// True when two distances (km) differ by strictly less than 3% of the larger value
    /// (e.g. 509 km vs 511 km).
    /// </summary>
    public static bool DistancesRoughMatchKm(double aKm, double bKm) =>
        RaceDistanceKm.RoughlyEqualSymmetric(aKm, bKm, RaceDistanceKm.DefaultSymmetricRoughFraction);

    private static bool AnyKmRoughlyIn(IEnumerable<double> values, double km) =>
        values.Any(v => DistancesRoughMatchKm(v, km));

    private static bool IsPureNumericCommaSeparatedDistance(string distance)
    {
        var tokens = distance.Split(',')
            .Select(t => t.Trim())
            .Where(t => !string.IsNullOrEmpty(t))
            .ToList();
        if (tokens.Count == 0) return false;
        foreach (var token in tokens)
        {
            var stripped = NumericMultiTokenSuffix.Replace(token, "").Trim();
            if (!double.TryParse(stripped, FloatKm, Inv, out _))
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
            var stripped = NumericMultiTokenSuffix.Replace(token, "").Trim();
            if (double.TryParse(stripped, FloatKm, Inv, out var value))
                numericValues.Add(value);
            else
                otherTokens.Add(token.ToLowerInvariant());
        }

        if (otherTokens.Count == 0)
            return string.Join(",", numericValues.OrderBy(v => v)
                .Select(v => v.ToString("G", Inv)));

        return string.Join(",", tokens.Select(t => t.ToLowerInvariant()));
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

        SourceDiscovery? bestSingle = null;
        double bestSingleDelta = double.MaxValue;
        SourceDiscovery? bestMulti = null;
        double bestMultiDelta = double.MaxValue;

        foreach (var (_, entry) in orderedDiscoveries)
        {
            var dists = ParseDistanceList(entry.Distance);
            if (dists.Count == 0) continue;

            var closestDelta = double.MaxValue;
            foreach (var d in dists)
            {
                if (!DistancesRoughMatchKm(d, routeKm.Value)) continue;
                var delta = Math.Abs(d - routeKm.Value);
                if (delta < closestDelta) closestDelta = delta;
            }

            if (closestDelta == double.MaxValue) continue;

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
    public static List<double> ParseDistanceList(string? distance) =>
        RaceDistanceKm.ParseCommaSeparatedKilometers(distance);

    /// <summary>Parses the first numeric km value from a distance string like "33 km" or "33 km, 21 km".</summary>
    public static double? ParseDistanceKm(string? distance) =>
        RaceDistanceKm.TryParsePrimarySegmentKilometers(distance);

    /// <summary>
    /// Adds every discovery-listed km value that <see cref="DistancesRoughMatchKm"/> with
    /// <paramref name="effectiveRouteKm"/> to <paramref name="claimedKm"/> (e.g. 163 and 164
    /// for a ~163 km GPX line).
    /// </summary>
    internal static void ClaimRoughlyMatchingDiscoveryDistances(
        double effectiveRouteKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries,
        HashSet<double> claimedKm)
    {
        foreach (var (_, entry) in flatDiscoveries)
        {
            foreach (var d in ParseDistanceList(entry.Distance))
            {
                if (DistancesRoughMatchKm(d, effectiveRouteKm))
                    claimedKm.Add(d);
            }
        }
    }

    private static double? ParseEffectiveRouteKmForClaiming(
        double? routeKm,
        Dictionary<string, dynamic> props)
    {
        if (routeKm.HasValue) return routeKm;
        if (!props.TryGetValue(RaceScrapeDiscovery.PropDistance, out var raw)) return null;
        return ParseDistanceKm(Convert.ToString(raw, Inv));
    }

    /// <summary>
    /// True when a scraped <see cref="LineString"/> race already represents <paramref name="km"/>
    /// within ~3%: any numeric segment in <see cref="RaceScrapeDiscovery.PropDistance"/> (comma list),
    /// else great-circle length of the polyline. Used to drop duplicate discovery points (and point
    /// fallbacks for second scraper rows) that echo the same ultra as a GPX route — not point-vs-point.
    /// </summary>
    internal static bool AssemblyAlreadyCoversRoughDistanceKm(double km, IReadOnlyList<StoredFeature> results)
    {
        foreach (var r in results)
        {
            if (r.Geometry is not LineString line)
                continue;
            if (LineRoughlyCoversKm(line, r.Properties, km))
                return true;
        }
        return false;
    }

    private static bool LineRoughlyCoversKm(LineString line, IDictionary<string, dynamic> props, double km)
    {
        if (props.TryGetValue(RaceScrapeDiscovery.PropDistance, out var raw))
        {
            var s = Convert.ToString(raw, Inv);
            if (!string.IsNullOrWhiteSpace(s))
            {
                foreach (var pk in ParseDistanceList(s))
                {
                    if (DistancesRoughMatchKm(pk, km))
                        return true;
                }
            }
        }

        var approxKm = ApproximateGreatCircleLineLengthKm(line);
        return approxKm.HasValue && DistancesRoughMatchKm(approxKm.Value, km);
    }

    private static double? ApproximateGreatCircleLineLengthKm(LineString line)
    {
        if (line.Coordinates is null)
            return null;

        using var e = line.Coordinates.GetEnumerator();
        if (!e.MoveNext())
            return null;

        var prev = e.Current;
        double sum = 0;
        while (e.MoveNext())
        {
            sum += HaversineKm(prev, e.Current);
            prev = e.Current;
        }

        return sum > 0 ? sum : null;
    }

    private static double HaversineKm(Position a, Position b)
    {
        const double earthRadiusKm = 6371.0;
        var lat1 = a.Latitude * (Math.PI / 180.0);
        var lat2 = b.Latitude * (Math.PI / 180.0);
        var dLat = (b.Latitude - a.Latitude) * (Math.PI / 180.0);
        var dLon = (b.Longitude - a.Longitude) * (Math.PI / 180.0);
        var h = Math.Sin(dLat / 2) * Math.Sin(dLat / 2)
                + Math.Cos(lat1) * Math.Cos(lat2) * Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        return 2 * earthRadiusKm * Math.Asin(Math.Min(1.0, Math.Sqrt(h)));
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
                if (trimmed.Length == 0) continue;
                if (RaceDistanceKm.TryParseCommaListTokenKilometers(trimmed, out var km))
                {
                    if (AnyKmRoughlyIn(seenKm, km)) continue;
                    seenKm.Add(km);
                    if (!AnyKmRoughlyIn(claimedKm, km))
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

            var allowedCount = Math.Max(1, CountRoughMatchingDiscoveryRouteSlots(group.Key.Value, flatDiscoveries));
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

    /// <summary>
    /// Singleton discovery rows whose sole km is not listed verbatim on any multi-distance row but
    /// still ~matches a token there (e.g. DUV 164 km vs runagain "…, 163 km") — one GPX slot.
    /// </summary>
    internal static int CountRedundantMeasurementSingletons(
        double canonicalRouteKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries)
    {
        var multiEntries = flatDiscoveries
            .Where(x => ParseDistanceList(x.Entry.Distance).Count >= 2)
            .Select(x => x.Entry)
            .ToList();
        if (multiEntries.Count == 0) return 0;

        var redundant = 0;
        foreach (var (_, entry) in flatDiscoveries)
        {
            var dists = ParseDistanceList(entry.Distance);
            if (dists.Count != 1) continue;
            var d = dists[0];
            if (!DistancesRoughMatchKm(d, canonicalRouteKm)) continue;

            foreach (var multi in multiEntries)
            {
                if (ReferenceEquals(entry, multi)) continue;
                var tokens = ParseDistanceList(multi.Distance);
                if (tokens.Any(t => Math.Abs(t - d) < 1e-9))
                    goto nextSingleton;
                if (tokens.Any(t => DistancesRoughMatchKm(t, d)))
                {
                    redundant++;
                    break;
                }
            }

        nextSingleton:
            ;
        }

        return redundant;
    }

    /// <summary>
    /// <see cref="CountMatchingDiscoveryEntries"/> minus singleton rows that only repeat a
    /// multi-distance list value under a different published km (see
    /// <see cref="CountRedundantMeasurementSingletons"/>).
    /// </summary>
    internal static int CountRoughMatchingDiscoveryRouteSlots(
        double canonicalRouteKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries)
    {
        var raw = CountMatchingDiscoveryEntries(canonicalRouteKm, flatDiscoveries);
        var sub = CountRedundantMeasurementSingletons(canonicalRouteKm, flatDiscoveries);
        return Math.Max(1, raw - sub);
    }
}
