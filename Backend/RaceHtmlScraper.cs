using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Backend;

// Stateless helpers for extracting structured data from scraped HTML pages and
// for parsing HTTP API responses that are only consumed by ScrapeRaceWorker.
public static partial class RaceHtmlScraper
{
    // Keywords used to identify links to course/route pages.
    private static readonly string[] CourseKeywords = ["course", "bana", "lopp", "läs mer", "see more"];

    // Keywords used to identify generic download links.
    private static readonly string[] DownloadKeywords = ["ladda ner", "hämta", "download"];

    // Extracts .gpx URLs from an HTML page.
    // Looks in href attributes and also in JSON-escaped script content.
    public static IReadOnlyCollection<Uri> ExtractGpxUrlsFromHtml(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var matches = HrefRegex().Matches(html).Select(m => m.Groups["href"].Value)
            .Concat(AbsoluteGpxRegex().Matches(html).Select(m => m.Groups["url"].Value))
            .Concat(RelativeGpxRegex().Matches(html).Select(m => m.Groups["url"].Value));

        return [.. matches
            .Select(UnescapeJsonSlash)
            .Select(url => Uri.TryCreate(pageUrl, url, out var parsed) ? parsed : null)
            .Where(uri => uri is { Scheme: "http" or "https" })
            .Cast<Uri>()
            .Where(uri => uri.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
            .Distinct()];
    }

    // Extracts links that are likely to lead to course/route detail pages.
    // A link matches if its href path or visible link text contains any of the course keywords
    // (case-insensitive partial match).
    public static IReadOnlyCollection<Uri> ExtractCourseLinksFromHtml(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var results = new List<Uri>();

        foreach (Match match in AnchorRegex().Matches(html))
        {
            var href = match.Groups["href"].Value;
            var text = HtmlTagRegex().Replace(match.Groups["text"].Value, " ").Trim();

            var isMatch = CourseKeywords.Any(kw =>
                href.Contains(kw, StringComparison.OrdinalIgnoreCase) ||
                text.Contains(kw, StringComparison.OrdinalIgnoreCase));

            if (!isMatch) continue;
            if (!Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)) continue;
            if (uri.Scheme is not ("http" or "https")) continue;
            if (seen.Add(uri.AbsoluteUri))
                results.Add(uri);
        }

        return results;
    }

    // Extracts GPX file links from an HTML page.
    // A link matches if its href ends with ".gpx" (with optional query string) OR
    // the visible link text contains the word "gpx" (case-insensitive).
    public static IReadOnlyCollection<Uri> ExtractGpxLinksFromHtml(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var results = new List<Uri>();

        // hrefs ending in .gpx (href attributes and script-embedded URLs)
        foreach (var uri in ExtractGpxUrlsFromHtml(html, pageUrl))
        {
            if (seen.Add(uri.AbsoluteUri))
                results.Add(uri);
        }

        // anchors where the visible text contains "gpx"
        foreach (Match match in AnchorRegex().Matches(html))
        {
            var href = match.Groups["href"].Value;
            var text = HtmlTagRegex().Replace(match.Groups["text"].Value, " ").Trim();

            if (!text.Contains("gpx", StringComparison.OrdinalIgnoreCase)) continue;
            if (!Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)) continue;
            if (uri.Scheme is not ("http" or "https")) continue;
            if (seen.Add(uri.AbsoluteUri))
                results.Add(uri);
        }

        return results;
    }

    // Extracts download links based on well-known download keywords in the visible link text.
    // Keywords: "Ladda ner", "Hämta", "Download" (case-insensitive partial match).
    public static IReadOnlyCollection<Uri> ExtractDownloadLinksFromHtml(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var results = new List<Uri>();

        foreach (Match match in AnchorRegex().Matches(html))
        {
            var href = match.Groups["href"].Value;
            var text = HtmlTagRegex().Replace(match.Groups["text"].Value, " ").Trim();

            var isMatch = DownloadKeywords.Any(kw =>
                text.Contains(kw, StringComparison.OrdinalIgnoreCase));

            if (!isMatch) continue;
            if (!Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)) continue;
            if (uri.Scheme is not ("http" or "https")) continue;
            if (seen.Add(uri.AbsoluteUri))
                results.Add(uri);
        }

        return results;
    }

    // Finds the "Site de la course" anchor in an HTML page and returns its resolved URL.
    // Used to extract the external race website URL from a TraceDeTrail event page.
    public static Uri? ExtractRaceSiteUrl(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        foreach (Match match in AnchorRegex().Matches(html))
        {
            var href = match.Groups["href"].Value;
            var text = HtmlTagRegex().Replace(match.Groups["text"].Value, " ").Trim();

            if (!text.Contains("Site de la course", StringComparison.OrdinalIgnoreCase))
                continue;

            if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri) && uri.Scheme is "http" or "https")
                return uri;
        }

        return null;
    }

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

    [GeneratedRegex("href\\s*=\\s*[\"'](?<href>[^\"']+)[\"']", RegexOptions.IgnoreCase)]
    private static partial Regex HrefRegex();

    // Matches both normal URLs (https://...) and JSON-escaped URLs (https:\/\/...).
    [GeneratedRegex("(?<url>https?:\\\\?/\\\\?/[^\"'\\s<>]+?\\.gpx(?:\\?[^\"'\\s<>]*)?)", RegexOptions.IgnoreCase)]
    private static partial Regex AbsoluteGpxRegex();

    [GeneratedRegex("(?<url>/[^\"'\\s<>]+?\\.gpx(?:\\?[^\"'\\s<>]*)?)", RegexOptions.IgnoreCase)]
    private static partial Regex RelativeGpxRegex();

    // Matches an <a> element capturing href attribute and inner text content.
    [GeneratedRegex(@"<a\b[^>]*\bhref\s*=\s*[""'](?<href>[^""']+)[""'][^>]*>(?<text>.*?)</a>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex AnchorRegex();

    // Strips all HTML tags so anchor inner content can be inspected as plain text.
    [GeneratedRegex(@"<[^>]+>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex HtmlTagRegex();
}
