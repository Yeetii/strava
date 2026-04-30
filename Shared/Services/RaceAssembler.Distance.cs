using System.Globalization;
using System.Text.RegularExpressions;
using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Services;

/// <summary>
/// Distance parsing, ~3% rough matching, discovery claiming, route deduplication, and GPX line
/// coverage helpers for <see cref="RaceAssembler"/>.
/// </summary>
public static partial class RaceAssembler
{
    private sealed class LineCoverageCandidate(StoredFeature feature, List<double> distancesKm, double? approximateKm)
    {
        public StoredFeature Feature { get; } = feature;
        public List<double> DistancesKm { get; } = distancesKm;
        public double? ApproximateKm { get; } = approximateKm;
    }

    /// <summary>Trailing unit when validating purely numeric multi-distance fields.</summary>
    private static readonly Regex NumericMultiTokenSuffix = new(@"(?i)\s*(km|k|mi|m)\s*$", RegexOptions.Compiled);

    private static readonly NumberStyles FloatKm = NumberStyles.Float;
    private static readonly CultureInfo Inv = CultureInfo.InvariantCulture;

    /// <summary>
    /// True when two distances (km) differ by strictly less than 3% of the larger value.
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
    /// Adds every discovery-listed km value that roughly matches
    /// <paramref name="effectiveRouteKm"/> to <paramref name="claimedKm"/>.
    /// </summary>
    public static void ClaimRoughlyMatchingDiscoveryDistances(
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
        if (!props.TryGetValue(PropDistance, out var raw)) return null;
        return ParseDistanceKm(Convert.ToString(raw, Inv));
    }

    /// <summary>
    /// True when a scraped <see cref="LineString"/> race already represents <paramref name="km"/>
    /// within ~3%.
    /// </summary>
    public static bool AssemblyAlreadyCoversRoughDistanceKm(double km, IReadOnlyList<StoredFeature> results)
    {
        var lines = results
            .Where(r => r.Geometry is LineString)
            .Select(BuildLineCoverageCandidate)
            .ToList();
        return AnyLineRoughlyCoversKm(km, lines);
    }

    private static bool LineRoughlyCoversKm(LineString line, IDictionary<string, dynamic> props, double km)
    {
        if (props.TryGetValue(PropDistance, out var raw))
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

    private static LineCoverageCandidate BuildLineCoverageCandidate(StoredFeature line)
    {
        var distancesKm = GetFeatureDistancesKm(line);
        var approximateKm = distancesKm.Count == 0 && line.Geometry is LineString geometry
            ? ApproximateGreatCircleLineLengthKm(geometry)
            : null;
        return new LineCoverageCandidate(line, distancesKm, approximateKm);
    }

    private static bool AnyLineRoughlyCoversKm(double km, IReadOnlyList<LineCoverageCandidate> lines)
    {
        foreach (var line in lines)
        {
            if (LineRoughlyCoversKm(line, km))
                return true;
        }

        return false;
    }

    private static bool LineRoughlyCoversKm(LineCoverageCandidate line, double km)
    {
        foreach (var distanceKm in line.DistancesKm)
        {
            if (DistancesRoughMatchKm(distanceKm, km))
                return true;
        }

        return line.ApproximateKm.HasValue && DistancesRoughMatchKm(line.ApproximateKm.Value, km);
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
    public static List<(double? Km, string Label)> GetUnclaimedDiscoveryDistances(
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
    /// </summary>
    public static Dictionary<double, double> BuildCanonicalRouteKmMap(List<double> sortedDistinctKm)
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
    /// still roughly matches a token there.
    /// </summary>
    public static int CountRedundantMeasurementSingletons(
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
    /// multi-distance list value under a different published km.
    /// </summary>
    public static int CountRoughMatchingDiscoveryRouteSlots(
        double canonicalRouteKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries)
    {
        var raw = CountMatchingDiscoveryEntries(canonicalRouteKm, flatDiscoveries);
        var sub = CountRedundantMeasurementSingletons(canonicalRouteKm, flatDiscoveries);
        return Math.Max(1, raw - sub);
    }

    /// <summary>
    /// Two-pass merge over the assembled features:
    /// <list type="number">
    ///   <item>Points whose distance matches a <see cref="LineString"/> are absorbed into that line.</item>
    ///   <item>Remaining same-distance <see cref="Point"/> pairs are merged.</item>
    /// </list>
    /// </summary>
    public static List<StoredFeature> MergeSameDistanceFeatures(List<StoredFeature> results)
    {
        if (results.Count <= 1) return results;

        var toRemove = new HashSet<string>(StringComparer.Ordinal);

        // Pass 1: absorb points into same-distance linestrings.
        var lines = results
            .Where(r => r.Geometry is LineString)
            .Select(BuildLineCoverageCandidate)
            .ToList();
        if (lines.Count > 0)
        {
            foreach (var point in results.Where(r => r.Geometry is Point))
            {
                var pointKms = GetFeatureDistancesKm(point);
                if (pointKms.Count == 0) continue;

                StoredFeature? bestLine = null;
                double bestDelta = double.MaxValue;

                foreach (var line in lines)
                {
                    foreach (var pkm in pointKms)
                    {
                        if (!LineRoughlyCoversKm(line, pkm)) continue;

                        var delta = line.DistancesKm.Count > 0
                            ? line.DistancesKm.Min(lkm => Math.Abs(lkm - pkm))
                            : line.ApproximateKm is { } approx
                                ? Math.Abs(approx - pkm)
                                : 0;

                        if (delta < bestDelta)
                        {
                            bestDelta = delta;
                            bestLine = line.Feature;
                        }
                    }
                }

                if (bestLine is null) continue;

                MergeProperties(bestLine.Properties, point.Properties, overwrite: true);
                toRemove.Add(point.FeatureId!);
            }
        }

        // Pass 2: merge same-distance points into the first surviving point.
        var remainingPoints = results
            .Where(r => r.Geometry is Point && !toRemove.Contains(r.FeatureId!))
            .ToList();

        if (remainingPoints.Count > 1)
        {
            for (int i = 0; i < remainingPoints.Count; i++)
            {
                var survivor = remainingPoints[i];
                if (toRemove.Contains(survivor.FeatureId!)) continue;

                var survivorKms = GetFeatureDistancesKm(survivor);
                if (survivorKms.Count == 0) continue;

                for (int j = i + 1; j < remainingPoints.Count; j++)
                {
                    var candidate = remainingPoints[j];
                    if (toRemove.Contains(candidate.FeatureId!)) continue;

                    var candidateKms = GetFeatureDistancesKm(candidate);
                    if (candidateKms.Count == 0) continue;

                    if (survivorKms.Any(skm => candidateKms.Any(ckm => DistancesRoughMatchKm(skm, ckm)))
                        && NamesCompatibleForMerge(survivor.Properties, candidate.Properties))
                    {
                        MergeProperties(survivor.Properties, candidate.Properties, overwrite: false);
                        toRemove.Add(candidate.FeatureId!);
                    }
                }
            }
        }

        return toRemove.Count == 0
            ? results
            : results.Where(r => !toRemove.Contains(r.FeatureId!)).ToList();
    }

    private static List<double> GetFeatureDistancesKm(StoredFeature feature)
    {
        if (!feature.Properties.TryGetValue(PropDistance, out var raw)) return [];
        var s = Convert.ToString(raw, Inv);
        return string.IsNullOrWhiteSpace(s) ? [] : ParseDistanceList(s);
    }

    private static bool NamesCompatibleForMerge(
        IDictionary<string, dynamic> aProps,
        IDictionary<string, dynamic> bProps)
    {
        var aName = aProps.TryGetValue(PropName, out var an)
            ? NormalizeNameKey(Convert.ToString(an, Inv)) : null;
        var bName = bProps.TryGetValue(PropName, out var bn)
            ? NormalizeNameKey(Convert.ToString(bn, Inv)) : null;

        if (aName is null || bName is null) return true;

        return string.Equals(aName, bName, StringComparison.Ordinal);
    }

    private static void MergeProperties(
        IDictionary<string, dynamic> targetProps,
        IDictionary<string, dynamic> sourceProps,
        bool overwrite)
    {
        foreach (var (key, value) in sourceProps)
        {
            if (key == PropDistance)
                continue;

            if (key == PropSources)
            {
                if (value is not List<string> sourceSources) continue;
                if (targetProps.TryGetValue(key, out var existing) && existing is List<string> targetSources)
                {
                    foreach (var url in sourceSources)
                        if (!targetSources.Contains(url, StringComparer.OrdinalIgnoreCase))
                            targetSources.Add(url);
                }
                else
                {
                    targetProps[key] = new List<string>(sourceSources);
                }
                continue;
            }

            if (overwrite || !targetProps.ContainsKey(key))
                targetProps[key] = value;
        }
    }

    /// <summary>
    /// When two or more assembled races share the same name, appends the distance label to each
    /// ambiguous name so users can tell them apart (e.g. "Grand Trail" → "Grand Trail (50 km)").
    /// </summary>
    public static void AppendDistanceToAmbiguousNames(List<StoredFeature> results)
    {
        if (results.Count <= 1) return;

        var byName = new Dictionary<string, List<StoredFeature>>(StringComparer.OrdinalIgnoreCase);
        foreach (var r in results)
        {
            if (!r.Properties.TryGetValue(PropName, out var nameRaw)) continue;
            var name = Convert.ToString(nameRaw, Inv)?.Trim();
            if (string.IsNullOrWhiteSpace(name)) continue;

            if (!byName.TryGetValue(name, out List<StoredFeature>? list))
                byName[name] = list = [];
            list.Add(r);
        }

        foreach (var (_, group) in byName)
        {
            if (group.Count <= 1) continue;

            foreach (var r in group)
            {
                if (!r.Properties.TryGetValue(PropDistance, out var distRaw)) continue;
                var dist = Convert.ToString(distRaw, Inv)?.Trim();
                if (string.IsNullOrWhiteSpace(dist)) continue;

                var currentName = Convert.ToString(r.Properties[PropName], Inv)!;
                r.Properties[PropName] = $"{currentName} ({dist})";
            }
        }
    }
}
