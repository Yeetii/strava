using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Backend.Scrapers;
using Shared.Services;

namespace Backend;

public static partial class RaceHtmlScraper
{
    // Parses the response from GET https://tracedetrail.fr/trace/getTraceItra/{id}
    // Coordinates are in EPSG:3857 (Web Mercator). Only "gpx"-tagged points are the primary route.
    // Returns WGS84 (longitude, latitude) positions and total elevation stats from the last point.
    public static TraceDeTrailTraceData ParseTraceDeTrailTrace(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return new TraceDeTrailTraceData([], null, null);

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        // Response is {"success":1,"trace":{...},"geometry":"[{...}]"}
        // The geometry field is a JSON-encoded string containing the points array.
        if (root.ValueKind == JsonValueKind.Object
            && TryGetPropertyIgnoreCase(root, "geometry", out var geometryEl)
            && geometryEl.ValueKind == JsonValueKind.String)
        {
            var geometryJson = geometryEl.GetString();
            if (string.IsNullOrWhiteSpace(geometryJson))
                return new TraceDeTrailTraceData([], null, null);

            double? distance = null;
            double? elevationGain = null;
            if (TryGetPropertyIgnoreCase(root, "trace", out var traceEl) && traceEl.ValueKind == JsonValueKind.Object)
            {
                if (TryGetPropertyIgnoreCase(traceEl, "distance", out var distEl) && distEl.ValueKind == JsonValueKind.String
                    && double.TryParse(distEl.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                    distance = d;
                if (TryGetPropertyIgnoreCase(traceEl, "dev_pos", out var gainEl) && gainEl.ValueKind == JsonValueKind.String
                    && double.TryParse(gainEl.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var g))
                    elevationGain = g;
            }

            var traceData = ParseTracePoints(geometryJson);
            return traceData with { TotalDistanceKm = distance ?? traceData.TotalDistanceKm, ElevationGain = elevationGain ?? traceData.ElevationGain };
        }

        // Fallback: root might already be the array
        if (root.ValueKind == JsonValueKind.Array)
            return ParseTracePoints(json);

        return new TraceDeTrailTraceData([], null, null);
    }

    private static TraceDeTrailTraceData ParseTracePoints(string pointsJson)
    {
        using var doc = JsonDocument.Parse(pointsJson);
        if (doc.RootElement.ValueKind != JsonValueKind.Array)
            return new TraceDeTrailTraceData([], null, null);

        const double EarthHalfCircumference = 20037508.34;
        var points = new List<(double Lng, double Lat)>();
        double? totalDistanceKm = null;
        double? elevationGain = null;

        foreach (var point in doc.RootElement.EnumerateArray())
        {
            if (point.ValueKind != JsonValueKind.Object)
                continue;

            // Only primary route points
            if (TryGetPropertyIgnoreCase(point, "o", out var tagEl) && tagEl.ValueKind == JsonValueKind.String &&
                !string.Equals(tagEl.GetString(), "gpx", StringComparison.OrdinalIgnoreCase))
                continue;

            if (!TryGetPropertyIgnoreCase(point, "lon", out var lonEl) || !lonEl.TryGetDouble(out var x))
                continue;
            if (!TryGetPropertyIgnoreCase(point, "lat", out var latEl) || !latEl.TryGetDouble(out var y))
                continue;

            var lng = x / EarthHalfCircumference * 180.0;
            var lat = Math.Atan(Math.Exp(y / EarthHalfCircumference * Math.PI)) * 360.0 / Math.PI - 90.0;
            points.Add((lng, lat));

            // Track cumulative stats from the last point
            if (TryGetPropertyIgnoreCase(point, "x", out var distEl) && distEl.TryGetDouble(out var dist))
                totalDistanceKm = dist;
            if (TryGetPropertyIgnoreCase(point, "dp", out var gainEl) && gainEl.TryGetDouble(out var gain))
                elevationGain = gain;
        }

        return new TraceDeTrailTraceData(points, totalDistanceKm, elevationGain);
    }

    private static bool TryGetPropertyIgnoreCase(JsonElement element, string propertyName, out JsonElement value)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static string UnescapeJsonSlash(string value) =>
        value.Replace("\\/", "/", StringComparison.Ordinal);

    // ── TraceDeTrail event page extractors ─────────────────────────────────

    /// <summary>
    /// Extracts the location text from the TraceDeTrail event page.
    /// Looks for <c>&lt;div id="eventLocalite"&gt;...text...&lt;/div&gt;</c>.
    /// </summary>
    public static string? ExtractTraceDeTrailLocation(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var match = EventLocaliteRegex().Match(html);
        if (!match.Success)
            return null;

        var text = HtmlTagRegex().Replace(match.Groups["content"].Value, " ").Trim();
        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    /// <summary>
    /// Extracts the maximum ITRA points from a TraceDeTrail event page by parsing image filenames
    /// like <c>itra_pts_race3.png</c> → 3.
    /// </summary>
    public static int? ExtractTraceDeTrailItraPoints(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        int max = 0;
        foreach (Match match in ItraPointsImageRegex().Matches(html))
        {
            if (int.TryParse(match.Groups["pts"].Value, NumberStyles.Integer,
                CultureInfo.InvariantCulture, out var pts) && pts > max)
                max = pts;
        }
        return max > 0 ? max : null;
    }

    /// <summary>
    /// Extracts ITRA points from an ITRA event page by parsing the point icons shown
    /// in the ITRA Points section.
    /// </summary>
    public static int? ExtractItraPoints(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var digitMatches = ItraPointsIconRegex().Matches(html)
            .Select(m => MapItraDigit(m.Groups["digit"].Value))
            .Where(v => v >= 0)
            .ToList();

        if (digitMatches.Count > 0)
            return digitMatches.Aggregate(0, (current, digit) => current * 10 + digit);

        var sectionMatch = ItraPointsSectionRegex().Match(html);
        var section = sectionMatch.Success ? sectionMatch.Groups["section"].Value : html;

        var numericMatch = ItraPointsNumericRegex().Match(section);
        if (numericMatch.Success && int.TryParse(numericMatch.Groups["value"].Value,
            NumberStyles.Integer, CultureInfo.InvariantCulture, out var points))
        {
            return points;
        }

        return null;
    }

    /// <summary>
    /// Extracts the maximum elevation gain (D+) from the event page.
    /// Looks for <c>&lt;i class="fas fa-arrow-circle-up"&gt;&lt;/i&gt; 4492 m</c> patterns.
    /// </summary>
    public static double? ExtractTraceDeTrailElevationGain(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        double max = 0;
        foreach (Match match in ElevationGainRegex().Matches(html))
        {
            if (double.TryParse(match.Groups["meters"].Value, NumberStyles.Float,
                CultureInfo.InvariantCulture, out var m) && m > max)
                max = m;
        }
        return max > 0 ? max : null;
    }

    // <div id="eventLocalite" ...>...content...</div>
    [GeneratedRegex(@"<div\b[^>]*\bid\s*=\s*[""']eventLocalite[""'][^>]*>(?<content>.*?)</div>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex EventLocaliteRegex();

    // ITRA page point section contents.
    [GeneratedRegex(@"<h5>\s*ITRA Points\s*</h5>(?<section>.*?)(?:</div>\s*</div>\s*</div>|</div>\s*</div>)", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex ItraPointsSectionRegex();

    // Matches ITRA digit icon filenames such as zero.svg, one.svg, two.svg.
    [GeneratedRegex(@"itra_numbers/icons/(?<digit>zero|one|two|three|four|five|six|seven|eight|nine)\.svg", RegexOptions.IgnoreCase)]
    private static partial Regex ItraPointsIconRegex();

    // Numeric fallback when the section contains a plain number.
    [GeneratedRegex(@"(?:ITRA\s*Points|itra\s*points)[^0-9]{0,20}?(?<value>\d{1,3})", RegexOptions.IgnoreCase)]
    private static partial Regex ItraPointsNumericRegex();

    // itra_pts_race3.png → captures "3"
    [GeneratedRegex(@"itra_pts_race(?<pts>\d+)\.png", RegexOptions.IgnoreCase)]
    private static partial Regex ItraPointsImageRegex();

    private static int MapItraDigit(string digit) => digit.ToLowerInvariant() switch
    {
        "zero" => 0,
        "one" => 1,
        "two" => 2,
        "three" => 3,
        "four" => 4,
        "five" => 5,
        "six" => 6,
        "seven" => 7,
        "eight" => 8,
        "nine" => 9,
        _ => -1
    };

    // <i class="fas fa-arrow-circle-up"></i> 4492 m
    [GeneratedRegex(@"fa-arrow-circle-up[""'][^>]*>\s*</i>\s*(?<meters>[\d\s]+?)\s*m\b", RegexOptions.IgnoreCase)]
    private static partial Regex ElevationGainRegex();

    // ── TraceDeTrail per-course extraction ──────────────────────────────────

    /// <summary>
    /// Extracts per-course data from a TraceDeTrail event page.
    /// Each course tab contains name, distance, D+, ITRA points, and a trace ID.
    /// </summary>
    public static IReadOnlyList<TraceDeTrailCourseInfo> ExtractTraceDeTrailCourses(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var courses = new List<TraceDeTrailCourseInfo>();
        var paneStarts = TabPaneStartRegex().Matches(html);
        if (paneStarts.Count == 0)
            return [];

        for (int i = 0; i < paneStarts.Count; i++)
        {
            var start = paneStarts[i].Index;
            var end = i + 1 < paneStarts.Count ? paneStarts[i + 1].Index : html.Length;
            var section = html[start..end];

            // Name: <div class="traceNom">42k</div>
            string? name = null;
            var nameMatch = TraceNomRegex().Match(section);
            if (nameMatch.Success)
                name = HtmlTagRegex().Replace(nameMatch.Groups["name"].Value, "").Trim();

            // Distance: <i class="fas fa-arrows-alt-h"></i> 41.49 km
            string? distance = null;
            var distMatch = TraceDistanceRegex().Match(section);
            if (distMatch.Success && double.TryParse(distMatch.Groups["dist"].Value,
                NumberStyles.Float, CultureInfo.InvariantCulture, out var distKm))
                distance = RaceScrapeDiscovery.FormatDistanceKm(distKm);

            // Elevation gain: reuse existing regex
            double? elevationGain = null;
            var elevMatch = ElevationGainRegex().Match(section);
            if (elevMatch.Success && double.TryParse(elevMatch.Groups["meters"].Value,
                NumberStyles.Float, CultureInfo.InvariantCulture, out var meters))
                elevationGain = meters;

            // ITRA points: reuse existing regex
            int? itraPoints = null;
            var itraMatch = ItraPointsImageRegex().Match(section);
            if (itraMatch.Success && int.TryParse(itraMatch.Groups["pts"].Value,
                NumberStyles.Integer, CultureInfo.InvariantCulture, out var pts))
                itraPoints = pts;

            // Trace ID from link: /en/trace/296087
            int? traceId = null;
            var traceMatch = TraceViewUrlRegex().Match(section);
            if (traceMatch.Success && int.TryParse(traceMatch.Groups["id"].Value,
                NumberStyles.Integer, CultureInfo.InvariantCulture, out var tid))
                traceId = tid;

            if (name is not null || distance is not null || traceId is not null)
                courses.Add(new(name, distance, elevationGain, itraPoints, traceId));
        }

        return courses;
    }

    // Matches the start of each tab-pane div (course section boundary)
    [GeneratedRegex(@"<div\b[^>]*\bclass\s*=\s*""[^""]*\btab-pane\b", RegexOptions.IgnoreCase)]
    private static partial Regex TabPaneStartRegex();

    // <div class="traceNom">42k</div>
    [GeneratedRegex(@"<div[^>]*\bclass\s*=\s*[""']traceNom[""'][^>]*>(?<name>.*?)</div>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex TraceNomRegex();

    // <i class="fas fa-arrows-alt-h"></i> 41.49 km
    [GeneratedRegex(@"fa-arrows-alt-h[""'][^>]*>\s*</i>\s*(?<dist>[\d.]+)\s*km", RegexOptions.IgnoreCase)]
    private static partial Regex TraceDistanceRegex();

    // tracedetrail.fr/en/trace/296087 or /fr/trace/296087
    [GeneratedRegex(@"tracedetrail\.fr/(?:en|fr)/trace/(?<id>\d+)", RegexOptions.IgnoreCase)]
    private static partial Regex TraceViewUrlRegex();
}
