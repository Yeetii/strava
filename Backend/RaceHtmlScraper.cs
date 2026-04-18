using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Backend;

// Stateless helpers for extracting structured data from scraped HTML pages and
// for parsing HTTP API responses that are only consumed by ScrapeRaceWorker.
public static partial class RaceHtmlScraper
{
    // Keywords used to identify links to course/route pages.
    private static readonly string[] CourseKeywords = [
        "course", "parcours", "parcour",
        "route", "trace", "tracé", "trajet",
        "map", "kart", "karta", "carte",
        "bana", "banan", "lopp", "sträcka", "strecke",
        "gpx", "track", "itinerary", "itinéraire",
        "läs mer", "see more", "read more", "en savoir plus", "lire la suite",
        "info", "details", "programme",
    ];

    // Keywords used to identify generic download links.
    private static readonly string[] DownloadKeywords = ["ladda ner", "hämta", "download"];

    /// <summary>
    /// Tries to extract a distance from a URL path segment, e.g. "/80-km", "/100k", "/marathon/", "/course-42km".
    /// Returns a normalised distance string like "80 km" or null.
    /// </summary>
    public static string? ExtractDistanceFromUrl(Uri url)
    {
        var path = Uri.UnescapeDataString(url.AbsolutePath);

        // Try numeric distance patterns: "80-km", "80km", "100k", "42.195-km", "10-miles"
        var match = UrlDistanceRegex().Match(path);
        if (match.Success)
        {
            var num = match.Groups["num"].Value;
            var unit = match.Groups["unit"].Value.ToLowerInvariant();
            if (double.TryParse(num, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                if (unit is "mi" or "miles" or "mile")
                    value *= 1.60934;
                return RaceScrapeDiscovery.FormatDistanceKm(value);
            }
        }

        // Try marathon keywords in path segments
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        foreach (var seg in segments)
        {
            var cleaned = seg.Replace('-', ' ').Replace('_', ' ');
            if (RaceScrapeDiscovery.TryParseMarathonKeyword(cleaned, out var km))
                return RaceScrapeDiscovery.FormatDistanceKm(km);
        }

        return null;
    }

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

            // Also match links whose URL path or text contains a distance pattern (e.g. "27k", "100-km").
            if (!isMatch)
                isMatch = UrlDistanceRegex().IsMatch(href) || LinkTextDistanceRegex().IsMatch(text);

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

    /// <summary>
    /// Extracts the most likely event name from HTML pages.
    /// Uses JSON-LD name, OG title, page title, heading text scored by size/frequency,
    /// and cross-referenced with the domain name.
    /// Supply the start page + any course page HTMLs so common terms can be weighted.
    /// </summary>
    public static string? ExtractEventName(Uri startUrl, string? startPageHtml, IReadOnlyList<(Uri Url, string Html)>? coursePages = null)
    {
        var allHtmls = new List<string>();
        if (!string.IsNullOrWhiteSpace(startPageHtml))
            allHtmls.Add(startPageHtml);
        if (coursePages is not null)
            foreach (var (_, cpHtml) in coursePages)
                if (!string.IsNullOrWhiteSpace(cpHtml))
                    allHtmls.Add(cpHtml);

        if (allHtmls.Count == 0)
            return null;

        var html = allHtmls[0]; // start page is primary

        // 1. JSON-LD "name" from Event/SportsEvent schema.
        var jsonLdName = JsonLdNameRegex().Match(html);
        if (jsonLdName.Success)
        {
            var name = CleanExtractedName(jsonLdName.Groups["name"].Value);
            if (name is not null) return name;
        }

        // 2. OG title / site_name meta tags — apply domain-aware segment picking.
        var domainWords = ExtractDomainWords(startUrl);
        foreach (Match m in OgTitleRegex().Matches(html))
        {
            var raw = CleanExtractedName(m.Groups["title"].Value);
            if (raw is null) continue;
            var segments = TitleSeparatorRegex().Split(raw)
                .Select(s => s.Trim())
                .Where(s => s.Length >= 3)
                .ToList();
            if (segments.Count > 1 && domainWords.Count > 0)
            {
                var domainHost = string.Join("", domainWords);
                foreach (var seg in segments)
                {
                    var segLower = seg.ToLowerInvariant();
                    var words = seg.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    var overlap = words.Count(w => domainWords.Contains(w.ToLowerInvariant()));
                    if (overlap == 0 && domainHost.Length >= 5
                        && (segLower.Replace(" ", "").Contains(domainHost)
                            || domainHost.Contains(segLower.Replace(" ", ""))))
                        overlap = 2;
                    if (overlap > 0)
                        return CleanExtractedName(seg) ?? raw;
                }
            }
            return segments.Count > 0 ? CleanExtractedName(segments[0]) ?? raw : raw;
        }

        // 3. Collect name candidates from all structured sources:
        //    domain (decoded, humanised), <title> segments, heading text.
        //    Then count how often each candidate appears across the page text
        //    and pick the most frequent one, preferring longer names at a small frequency cost.

        var candidates = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Domain-derived candidate.
        var humanised = HumaniseDomain(startUrl);
        if (humanised is not null)
            candidates.Add(humanised);

        // <title> tag segments.
        var titleMatch = TitleTagRegex().Match(html);
        if (titleMatch.Success)
        {
            var raw = HtmlTagRegex().Replace(titleMatch.Groups["title"].Value, "").Trim();
            raw = System.Net.WebUtility.HtmlDecode(raw);
            foreach (var seg in TitleSeparatorRegex().Split(raw))
            {
                var cleaned = CleanExtractedName(seg.Trim());
                if (cleaned is not null && cleaned.Length >= 3 && cleaned.Length <= 80)
                    candidates.Add(cleaned);
            }
        }

        // Heading text from all pages.
        foreach (var pageHtml in allHtmls)
        {
            foreach (Match m in HeadingRegex().Matches(pageHtml))
            {
                var text = HtmlTagRegex().Replace(m.Groups["text"].Value, "").Trim();
                text = CollapseWhitespaceRegex().Replace(text, " ");
                if (string.IsNullOrWhiteSpace(text) || text.Length < 3 || text.Length > 80)
                    continue;
                if (VisibleDateRegex().IsMatch(text)) continue;
                if (UrlDistanceRegex().IsMatch(text)) continue;
                if (NavigationTerms.Any(t => text.Equals(t, StringComparison.OrdinalIgnoreCase)))
                    continue;
                var cleaned = CleanExtractedName(text);
                if (cleaned is not null)
                    candidates.Add(cleaned);
            }
        }

        if (candidates.Count == 0)
            return humanised;

        // Count occurrences of each candidate in the start page visible text.
        var visibleText = HtmlTagRegex().Replace(html, " ");
        var candidateCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var candidate in candidates)
        {
            var count = 0;
            var idx = 0;
            while ((idx = visibleText.IndexOf(candidate, idx, StringComparison.OrdinalIgnoreCase)) >= 0)
            {
                count++;
                idx += candidate.Length;
            }
            candidateCounts[candidate] = count;
        }

        // Pick the best: highest count, but prefer longer candidates when counts are close.
        // "EcoTrail Trondheim" (count 8) beats "EcoTrail" (count 10) because it's longer
        // and only slightly less frequent.
        var maxCount = candidateCounts.Values.Max();
        var threshold = Math.Max(2, maxCount / 2); // must appear at least twice and at least half as often as the top

        var viable = candidateCounts
            .Where(kv => kv.Value >= threshold)
            .OrderByDescending(kv => kv.Key.Length) // prefer longer names
            .ThenByDescending(kv => kv.Value)       // then by frequency
            .ToList();

        if (viable.Count > 0)
        {
            // Among viable candidates, check if a longer one contains a shorter high-frequency one.
            // If so, the longer one is more specific and better.
            var best = viable[0];
            return CleanExtractedName(best.Key) ?? humanised;
        }

        // 4. Fall back to domain-derived name.
        return humanised;
    }

    private static readonly string[] NavigationTerms = [
        "home", "menu", "contact", "about", "login", "register", "results",
        "inscription", "résultats", "accueil", "hem", "start",
    ];

    private static string? CleanExtractedName(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var text = System.Net.WebUtility.HtmlDecode(raw).Trim();
        text = CollapseWhitespaceRegex().Replace(text, " ");
        // Strip trailing boilerplate like "- Home", "| Official Site".
        text = TitleSeparatorRegex().Split(text)[0].Trim();
        return text.Length >= 3 ? text : null;
    }

    private static string DecodePunycode(string host)
    {
        try
        {
            var idn = new System.Globalization.IdnMapping();
            return idn.GetUnicode(host);
        }
        catch
        {
            return host;
        }
    }

    private static HashSet<string> ExtractDomainWords(Uri url)
    {
        // "www.ecotrailtrondheim.com" → ["ecotrailtrondheim"]
        // "ultra-trail-reunion.com" → ["ultra", "trail", "reunion"]
        // "xn--ndalsneslpet-scb9y.no" → ["åndalsneslöpet"]
        var host = DecodePunycode(url.Host.ToLowerInvariant());
        if (host.StartsWith("www.")) host = host[4..];
        var tldIdx = host.LastIndexOf('.');
        if (tldIdx > 0) host = host[..tldIdx];
        // Split on dots and hyphens.
        return host.Split(['.', '-'], StringSplitOptions.RemoveEmptyEntries)
            .Where(w => w.Length >= 3)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }

    private static string? HumaniseDomain(Uri url)
    {
        var host = DecodePunycode(url.Host.ToLowerInvariant());
        if (host.StartsWith("www.")) host = host[4..];
        var tldIdx = host.LastIndexOf('.');
        if (tldIdx > 0) host = host[..tldIdx];
        // "ecotrail-trondheim" → "Ecotrail Trondheim"
        var name = host.Replace('-', ' ').Replace('_', ' ');
        name = CollapseWhitespaceRegex().Replace(name, " ").Trim();
        if (name.Length < 3) return null;
        return CultureInfo.InvariantCulture.TextInfo.ToTitleCase(name);
    }

    /// <summary>
    /// Extracts the most likely race/event date from an HTML page.
    /// Tries structured sources first (JSON-LD startDate, &lt;time datetime&gt;, meta tags),
    /// then falls back to visible text patterns.
    /// Returns a normalized YYYY-MM-DD string or null.
    /// </summary>
    public static string? ExtractDate(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        // 1. JSON-LD startDate (most reliable).
        foreach (Match m in JsonLdStartDateRegex().Matches(html))
        {
            var date = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(m.Groups["date"].Value.Trim());
            if (date is not null) return date;
        }

        // 2. <time datetime="..."> element.
        foreach (Match m in TimeDatetimeRegex().Matches(html))
        {
            var date = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(m.Groups["dt"].Value.Trim());
            if (date is not null) return date;
        }

        // 3. <meta> tags with date-related names/properties.
        foreach (Match m in MetaDateRegex().Matches(html))
        {
            var date = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(m.Groups["date"].Value.Trim());
            if (date is not null) return date;
        }

        // 4. Visible text: score each candidate by heading context, font-size hints, and frequency.
        // Strip script/style blocks so JSON-LD dates and CSS aren't matched as visible text.
        var visibleHtml = ScriptStyleRegex().Replace(html, " ");
        var candidates = new Dictionary<string, int>(StringComparer.Ordinal); // normalized date → score
        foreach (Match m in VisibleDateRegex().Matches(visibleHtml))
        {
            var normalized = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(m.Value.Trim());
            if (normalized is null) continue;

            int score = 1; // base occurrence score

            // Check surrounding context for heading tags or large font-size.
            var contextStart = Math.Max(0, m.Index - 200);
            var contextLen = Math.Min(visibleHtml.Length - contextStart, m.Length + 400);
            var context = visibleHtml.AsSpan(contextStart, contextLen);

            if (context.Contains("<h1", StringComparison.OrdinalIgnoreCase))
                score += 10;
            else if (context.Contains("<h2", StringComparison.OrdinalIgnoreCase))
                score += 7;
            else if (context.Contains("<h3", StringComparison.OrdinalIgnoreCase))
                score += 5;
            else if (context.Contains("<h4", StringComparison.OrdinalIgnoreCase)
                  || context.Contains("<h5", StringComparison.OrdinalIgnoreCase)
                  || context.Contains("<h6", StringComparison.OrdinalIgnoreCase))
                score += 3;

            // Large inline font-size (≥ 20px / 1.5em/rem) hints at prominent text.
            var fontMatch = FontSizeRegex().Match(context.ToString());
            if (fontMatch.Success && double.TryParse(fontMatch.Groups["size"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var size))
            {
                var unit = fontMatch.Groups["unit"].Value.ToLowerInvariant();
                var px = unit switch { "em" or "rem" => size * 16, _ => size };
                if (px >= 24) score += 6;
                else if (px >= 20) score += 3;
            }

            candidates.TryGetValue(normalized, out var existing);
            candidates[normalized] = existing + score;
        }

        if (candidates.Count > 0)
            return candidates.MaxBy(kv => kv.Value).Key;

        return null;
    }

    /// <summary>
    /// Returns the most prominent image URL from an HTML page.
    /// Prefers large images (by explicit width/height), Open Graph meta images,
    /// and images whose src/alt/class hints at a hero/banner. Skips tiny icons, trackers, and data URIs.
    /// </summary>
    public static Uri? ExtractProminentImage(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        // 1. Try Open Graph / Twitter meta image first — usually the hero.
        Uri? ogCandidate = null;
        foreach (Match m in OgImageRegex().Matches(html))
        {
            var content = m.Groups["url"].Value.Trim();
            if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(content), out var uri)
                && uri.Scheme is "http" or "https" && !IsTrackingPixel(uri))
            {
                // If the OG image looks like a logo, keep it as fallback instead of returning immediately.
                if (uri.AbsolutePath.Contains("logo", StringComparison.OrdinalIgnoreCase))
                {
                    ogCandidate ??= uri;
                    continue;
                }
                return uri;
            }
        }

        // 2. Score all <img> tags and pick the best.
        Uri? best = null;
        var bestScore = -1;

        foreach (Match m in ImgRegex().Matches(html))
        {
            var tag = m.Value;
            var src = ImgSrcRegex().Match(tag).Groups["src"].Value;
            if (string.IsNullOrWhiteSpace(src) || src.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
                continue;
            if (!Uri.TryCreate(pageUrl, UnescapeJsonSlash(src), out var uri)
                || uri.Scheme is not ("http" or "https"))
                continue;
            if (IsTrackingPixel(uri)) continue;

            var score = ScoreImage(tag, uri);
            if (score > bestScore)
            {
                bestScore = score;
                best = uri;
            }
        }

        return best ?? ogCandidate;
    }

    /// <summary>
    /// Extracts a logo image from the page. Looks for images whose src, alt, or class
    /// contains "logo", or inside elements with "logo" in their class.
    /// </summary>
    public static Uri? ExtractLogo(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        // Look for <img> tags where src/alt/class contain "logo".
        foreach (Match m in ImgRegex().Matches(html))
        {
            var tag = m.Value;
            if (!tag.Contains("logo", StringComparison.OrdinalIgnoreCase)) continue;

            var src = ImgSrcRegex().Match(tag).Groups["src"].Value;
            if (string.IsNullOrWhiteSpace(src) || src.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
                continue;
            if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(src), out var uri)
                && uri.Scheme is "http" or "https")
                return uri;
        }

        // Fallback: look for <link rel="icon"> (favicon).
        foreach (Match m in LinkIconRegex().Matches(html))
        {
            var href = m.Groups["href"].Value;
            if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)
                && uri.Scheme is "http" or "https")
                return uri;
        }

        return null;
    }

    private static bool IsTrackingPixel(Uri uri)
    {
        var path = uri.AbsolutePath;
        return path.Contains("pixel", StringComparison.OrdinalIgnoreCase)
            || path.Contains("tracker", StringComparison.OrdinalIgnoreCase)
            || path.Contains("spacer", StringComparison.OrdinalIgnoreCase)
            || path.EndsWith(".gif", StringComparison.OrdinalIgnoreCase) && path.Contains("1x1", StringComparison.OrdinalIgnoreCase);
    }

    private static int ScoreImage(string imgTag, Uri uri)
    {
        var score = 0;
        var tagLower = imgTag.ToLowerInvariant();
        var path = uri.AbsolutePath.ToLowerInvariant();

        // Penalize known non-content images.
        if (tagLower.Contains("icon") || tagLower.Contains("avatar") || tagLower.Contains("emoji"))
            return -1;
        if (path.Contains("icon") || path.Contains("avatar") || path.Contains("emoji")
            || path.Contains("spinner") || path.Contains("loading"))
            return -1;

        // Penalize logos — they exist but shouldn't win over real content images.
        if (tagLower.Contains("logo") || path.Contains("logo"))
            score -= 5;

        // Explicit dimensions — larger = more prominent.
        var widthMatch = ImgWidthRegex().Match(imgTag);
        var heightMatch = ImgHeightRegex().Match(imgTag);
        if (widthMatch.Success && int.TryParse(widthMatch.Groups["val"].Value, out var w))
        {
            if (w < 50) return -1; // tiny
            score += w > 400 ? 3 : w > 200 ? 2 : 1;
        }
        if (heightMatch.Success && int.TryParse(heightMatch.Groups["val"].Value, out var h))
        {
            if (h < 50) return -1;
            score += h > 300 ? 3 : h > 150 ? 2 : 1;
        }

        // Hero/banner hints.
        if (tagLower.Contains("hero") || tagLower.Contains("banner") || tagLower.Contains("featured")
            || tagLower.Contains("cover") || tagLower.Contains("header-image"))
            score += 5;

        return score;
    }

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

    // Removes <script>...</script>, <style>...</style> blocks, and HTML comments.
    [GeneratedRegex(@"<(?:script|style)\b[^>]*>[\s\S]*?</(?:script|style)>|<!--[\s\S]*?-->", RegexOptions.IgnoreCase)]
    private static partial Regex ScriptStyleRegex();

    // Matches <meta property="og:image" content="..."> and twitter:image variants.
    [GeneratedRegex(@"<meta\b[^>]*(?:property|name)\s*=\s*[""'](?:og:image|twitter:image)[""'][^>]*content\s*=\s*[""'](?<url>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex OgImageRegex();

    // Matches <img ...> tags.
    [GeneratedRegex(@"<img\b[^>]*>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex ImgRegex();

    // Extracts the src attribute from an <img> tag.
    [GeneratedRegex(@"src\s*=\s*[""'](?<src>[^""']+)[""']", RegexOptions.IgnoreCase)]
    private static partial Regex ImgSrcRegex();

    // Matches width attribute or width in style.
    [GeneratedRegex(@"width\s*[:=]\s*[""']?(?<val>\d+)", RegexOptions.IgnoreCase)]
    private static partial Regex ImgWidthRegex();

    // Matches height attribute or height in style.
    [GeneratedRegex(@"height\s*[:=]\s*[""']?(?<val>\d+)", RegexOptions.IgnoreCase)]
    private static partial Regex ImgHeightRegex();

    // Matches <link rel="icon" href="..."> or <link rel="shortcut icon" href="...">.
    [GeneratedRegex(@"<link\b[^>]*rel\s*=\s*[""'](?:shortcut\s+)?icon[""'][^>]*href\s*=\s*[""'](?<href>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex LinkIconRegex();

    // Matches "startDate" (or "dateStart", "start_date") values in JSON-LD or inline JSON.
    [GeneratedRegex(@"""(?:startDate|dateStart|start_date)""\s*:\s*""(?<date>[^""]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex JsonLdStartDateRegex();

    // Matches <time datetime="..."> elements.
    [GeneratedRegex(@"<time\b[^>]*\bdatetime\s*=\s*[""'](?<dt>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex TimeDatetimeRegex();

    // Matches <meta> tags with date-related names (e.g. startDate, event:start_date, date).
    [GeneratedRegex(@"<meta\b[^>]*(?:property|name)\s*=\s*[""'](?:.*?date.*?)[""'][^>]*content\s*=\s*[""'](?<date>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex MetaDateRegex();

    // Month name pattern covering English, Norwegian, Swedish, German, French.
    private const string MonthPattern =
        @"jan(?:uary|vier|uar|uari)?|feb(?:ruary|rier|ruar|ruari)?|mar(?:ch|s|z|ts)?|märz"
        + @"|apr(?:il)?|ma[iyj]|jun[ei]?|juin|jul[iy]?|juillet|aug(?:ust|usti)?|août"
        + @"|sep(?:tembre?)?|o[ck]t(?:ob(?:er|re))?|nov(?:emb(?:er|re))?|de[csz](?:emb(?:er|re))?|décembre";

    // Visible date patterns, multilingual:
    //   "14 September 2025", "14. september 2025", "lørdag den 24. mai 2025",
    //   "September 14, 2025", "14/09/2025", "14.09.2025", "2025-09-14".
    [GeneratedRegex(
        @"\b\d{1,2}\.?\s+(?:" + MonthPattern + @")\s+\d{4}\b"               // 14 September 2025 / 14. mai 2025
        + @"|\b(?:" + MonthPattern + @")\s+\d{1,2},?\s+\d{4}\b"             // September 14, 2025
        + @"|\b\d{1,2}[/\.]\d{1,2}[/\.]\d{4}\b"                            // 14/09/2025 or 14.09.2025
        + @"|\b\d{4}-\d{2}-\d{2}\b",                                        // 2025-09-14
        RegexOptions.IgnoreCase)]
    private static partial Regex VisibleDateRegex();

    // Matches distance patterns in URL paths: "80-km", "80km", "100k", "42.195-km", "10-miles".
    [GeneratedRegex(@"(?:^|[/\-_])(?<num>\d+(?:\.\d+)?)\s*-?\s*(?<unit>km|k|mi(?:les?)?|miles?)(?=[/\-_?#]|$)", RegexOptions.IgnoreCase)]
    private static partial Regex UrlDistanceRegex();

    // Matches distance in link text: "27K", "100 km", "45K", "10 miles".
    [GeneratedRegex(@"\b\d+(?:\.\d+)?\s*(?:km|k|mi(?:les?)?)\b", RegexOptions.IgnoreCase)]
    private static partial Regex LinkTextDistanceRegex();

    // Matches inline font-size declarations like "font-size: 24px" or "font-size:1.5em".
    [GeneratedRegex(@"font-size\s*:\s*(?<size>\d+(?:\.\d+)?)\s*(?<unit>px|em|rem)", RegexOptions.IgnoreCase)]
    private static partial Regex FontSizeRegex();

    // Matches JSON-LD "name" field (Event, SportsEvent schemas).
    [GeneratedRegex(@"""name""\s*:\s*""(?<name>[^""]{3,100})""", RegexOptions.IgnoreCase)]
    private static partial Regex JsonLdNameRegex();

    // Matches <meta property="og:title" content="..."> and og:site_name (either attribute order).
    [GeneratedRegex(@"<meta\b[^>]*(?:(?:property|name)\s*=\s*[""'](?:og:title|og:site_name)[""'][^>]*content\s*=\s*[""'](?<title>[^""']+)[""']|content\s*=\s*[""'](?<title>[^""']+)[""'][^>]*(?:property|name)\s*=\s*[""'](?:og:title|og:site_name)[""'])", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex OgTitleRegex();

    // Matches <title>...</title>.
    [GeneratedRegex(@"<title[^>]*>(?<title>[^<]+)</title>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex TitleTagRegex();

    // Matches heading tags <h1>–<h6>, capturing level and inner HTML.
    [GeneratedRegex(@"<h(?<level>[1-6])\b[^>]*>(?<text>.*?)</h\k<level>>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex HeadingRegex();

    // Common title separators: " - ", " | ", " — ", " :: ".
    [GeneratedRegex(@"\s*[\|\-—–:]{1,2}\s*", RegexOptions.None)]
    private static partial Regex TitleSeparatorRegex();

    // Collapses multiple whitespace chars into a single space.
    [GeneratedRegex(@"\s+")]
    private static partial Regex CollapseWhitespaceRegex();

    // Matches elevation gain keywords followed by a number, or a number followed by elevation keywords.
    // Patterns: "elevation gain: 1200 m", "Elevation Gain ↙ 900 m ↘ 1050 m", "höjdmeter: 1200", "D+ 1200m".
    // Allows up to 40 chars gap between keyword and number (tags are stripped first).
    private static readonly string[] ElevationKeywords =
        ["elevation gain", "ascent", "dénivelé positif", "dénivelé", "denivelé", "denivele",
         "aufstieg", "höjdmeter", "höhenmeter", "hohenmeter", "stigning", "d+"];

    [GeneratedRegex(@"\b(\d[\d\s]{0,6}\d|\d{1,5})\s*(?:m\b|meter\b|hm\b)", RegexOptions.IgnoreCase)]
    private static partial Regex ElevationNumberRegex();

    /// <summary>
    /// Extracts elevation gain (in metres) from visible text in an HTML page.
    /// Finds the largest plausible value (1–99999 m) near an elevation keyword.
    /// </summary>
    public static double? ExtractElevationGain(string? html)
    {
        if (string.IsNullOrWhiteSpace(html)) return null;

        var text = HtmlTagRegex().Replace(html, " ");

        double? best = null;
        foreach (var keyword in ElevationKeywords)
        {
            var kwIdx = text.IndexOf(keyword, StringComparison.OrdinalIgnoreCase);
            if (kwIdx < 0) continue;

            // Search a window around the keyword (100 chars after the keyword end, 40 before).
            var searchStart = Math.Max(0, kwIdx - 40);
            var searchEnd = Math.Min(text.Length, kwIdx + keyword.Length + 100);
            var window = text[searchStart..searchEnd];

            foreach (Match match in ElevationNumberRegex().Matches(window))
            {
                var raw = match.Groups[1].Value.Replace(" ", "");
                if (double.TryParse(raw, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var metres)
                    && metres >= 1 && metres <= 99999)
                {
                    // Take the largest value near any keyword (likely total ascent rather than partial).
                    if (!best.HasValue || metres > best.Value)
                        best = metres;
                }
            }
        }
        return best;
    }
}
