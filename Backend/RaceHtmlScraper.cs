using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Backend;

// Stateless helpers for extracting structured data from scraped HTML pages and
// for parsing HTTP API responses that are only consumed by ScrapeRaceWorker.
public static partial class RaceHtmlScraper
{
    public record PriceInfo(string Amount, string Currency);

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

    // Links matching a course keyword but also matching one of these are skipped
    // (result pages, start lists, registration, etc.).
    private static readonly string[] CourseExcludeKeywords = [
        "resultat", "result", "startlist", "startlista", "cart", "varukorg", 
        "villkor", "terms", "kontakt", "contact",
        "blogg", "blog", "nyheter", "news", "tips",
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

        // Try numeric distance patterns: "80-km", "80km", "100k", "100M", "42.195-km", "10-miles"
        var match = UrlDistanceRegex().Match(path);
        if (match.Success)
        {
            var num = match.Groups["num"].Value;
            var unitRaw = match.Groups["unit"].Value;
            var unit = unitRaw.ToLowerInvariant();
            if (double.TryParse(num, NumberStyles.Float,
                CultureInfo.InvariantCulture, out var value))
            {
                if (unitRaw == "M" || unit is "mi" or "miles" or "mile")
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

    /// <summary>
    /// Extracts distinct distance strings from the visible text of an HTML page.
    /// Returns normalised "X km" strings (e.g. "27 km", "100 km").
    /// </summary>
    public static IReadOnlyList<string> ExtractDistancesFromContent(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        // Strip <head>, script/style blocks so only visible content remains.
        var clean = HeadSectionRegex().Replace(html, " ");
        clean = ScriptStyleRegex().Replace(clean, " ");
        var visibleText = HtmlTagRegex().Replace(clean, " ");

        var seen = new HashSet<int>(); // rounded km values for dedup
        var results = new List<string>();
        foreach (Match m in ContentDistanceRegex().Matches(visibleText))
        {
            var num = m.Groups["num"].Value;
            var unitRaw = m.Groups["unit"].Value;
            var unit = unitRaw.ToLowerInvariant();
            if (!double.TryParse(num, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
                continue;
            if (value < 1 || value > 500) continue; // filter out noise
            if (unitRaw == "M" || unit is "mi" or "miles" or "mile")
                value *= 1.60934;
            var rounded = (int)Math.Round(value);
            if (seen.Add(rounded))
                results.Add(RaceScrapeDiscovery.FormatDistanceKm(value));
        }
        return results;
    }

    private static double? ExtractPageDistanceKm(string html)
    {
        double? best = null;
        foreach (var distance in ExtractDistancesFromContent(html))
        {
            var km = AssembleRaceWorker.ParseDistanceKm(distance);
            if (km is null) continue;
            if (!best.HasValue || km.Value > best.Value)
                best = km.Value;
        }
        return best;
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
            if (href.StartsWith('#')) continue; // same-page anchor — already have the page content
            var text = HtmlTagRegex().Replace(match.Groups["text"].Value, " ").Trim();

            var isMatch = CourseKeywords.Any(kw =>
                href.Contains(kw, StringComparison.OrdinalIgnoreCase) ||
                text.Contains(kw, StringComparison.OrdinalIgnoreCase));

            // Also match links whose URL path or text contains a distance pattern (e.g. "27k", "100-km").
            if (!isMatch)
                isMatch = UrlDistanceRegex().IsMatch(href) || LinkTextDistanceRegex().IsMatch(text);

            if (!isMatch) continue;

            // Skip links that match exclude keywords (results, start lists, etc.).
            var isExcluded = CourseExcludeKeywords.Any(kw =>
                href.Contains(kw, StringComparison.OrdinalIgnoreCase) ||
                text.Contains(kw, StringComparison.OrdinalIgnoreCase));
            if (isExcluded) continue;
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
            if (href.StartsWith('#')) continue; // same-page anchor — already have the page content
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
            if (href.StartsWith('#')) continue; // same-page anchor
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

            var unescapedHref = UnescapeJsonSlash(href);
            // If href looks like a domain (e.g. www.example.com) but is missing scheme, treat as https://
            if (!unescapedHref.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
                !unescapedHref.StartsWith("https://", StringComparison.OrdinalIgnoreCase) &&
                System.Text.RegularExpressions.Regex.IsMatch(unescapedHref, @"^[\w.-]+\.[a-z]{2,}(\/.*)?$", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            {
                unescapedHref = "https://" + unescapedHref;
            }
            if (Uri.TryCreate(pageUrl, unescapedHref, out var uri) && uri.Scheme is "http" or "https")
                return uri;
        }

        return null;
    }

    public static Uri? ExtractDuvEventWebPageUrl(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var webPageRowMatch = DuvWebPageRowRegex().Match(html);
        if (webPageRowMatch.Success)
        {
            var rowHtml = webPageRowMatch.Groups[1].Value;
            foreach (Match anchorMatch in AnchorRegex().Matches(rowHtml))
            {
                var href = anchorMatch.Groups["href"].Value;
                if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri) && uri.Scheme is "http" or "https")
                    return uri;
            }
        }

        // Fallback: return the first external anchor URL on the page.
        foreach (Match anchorMatch in AnchorRegex().Matches(html))
        {
            var href = anchorMatch.Groups["href"].Value;
            if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)
                && uri.Scheme is "http" or "https"
                && !string.Equals(uri.Host, pageUrl.Host, StringComparison.OrdinalIgnoreCase))
            {
                return uri;
            }
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

        // 1. JSON-LD "name" from Event/SportsEvent schema — only look inside ld+json blocks.
        foreach (Match ldBlock in LdJsonBlockRegex().Matches(html))
        {
            var jsonLdName = JsonLdNameRegex().Match(ldBlock.Groups["json"].Value);
            if (jsonLdName.Success)
            {
                var name = CleanExtractedName(jsonLdName.Groups["name"].Value);
                if (name is not null) return name;
            }
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
                    var segNorm = StripDiacritics(segLower);
                    var words = seg.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                    var overlap = words.Count(w =>
                        domainWords.Contains(w.ToLowerInvariant())
                        || domainWords.Contains(StripDiacritics(w.ToLowerInvariant())));
                    if (overlap == 0 && domainHost.Length >= 5)
                    {
                        var segCompact = segNorm.Replace(" ", "");
                        if (segCompact.Contains(domainHost) || domainHost.Contains(segCompact))
                            overlap = 2;
                    }
                    if (overlap > 0)
                        return CleanExtractedName(seg) ?? raw;
                }
            }
            // Fallback: pick the first non-navigation segment, preferring domain-matching ones.
            var fallback = segments
                .Where(s => !NavigationTerms.Any(t => s.Equals(t, StringComparison.OrdinalIgnoreCase)))
                .ToList();
            if (fallback.Count > 0)
            {
                var domainMatch = fallback.FirstOrDefault(s => HasDomainAffinity(s, domainWords));
                var pick = domainMatch ?? fallback[0];
                return CleanExtractedName(pick) ?? raw;
            }
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
                if (cleaned is not null && cleaned.Length >= 3 && cleaned.Length <= 80
                    && !NavigationTerms.Any(t => cleaned.Equals(t, StringComparison.OrdinalIgnoreCase)))
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
            .OrderByDescending(kv => HasDomainAffinity(kv.Key, domainWords) ? 1 : 0)
            .ThenByDescending(kv => kv.Key.Length) // prefer longer names
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
        "home", "hjem", "menu", "contact", "about", "login", "register", "results",
        "inscription", "résultats", "accueil", "hem", "start",
    ];

    /// <summary>True when the candidate text overlaps with the domain name (diacritic-insensitive).</summary>
    private static bool HasDomainAffinity(string candidate, HashSet<string> domainWords)
    {
        if (domainWords.Count == 0) return false;
        var words = candidate.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (words.Any(w => domainWords.Contains(w.ToLowerInvariant())
                        || domainWords.Contains(StripDiacritics(w.ToLowerInvariant()))))
            return true;
        var domainHost = string.Join("", domainWords);
        if (domainHost.Length < 5) return false;
        var compact = StripDiacritics(candidate.ToLowerInvariant()).Replace(" ", "");
        return compact.Contains(domainHost) || domainHost.Contains(compact);
    }

    private static string? CleanExtractedName(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var text = System.Net.WebUtility.HtmlDecode(raw).Trim();
        // Decode JSON unicode escapes like \u00e4 → ä (common in JSON-LD / JS-embedded content).
        text = JsonUnicodeEscapeRegex().Replace(text, m =>
            ((char)int.Parse(m.Groups[1].Value, NumberStyles.HexNumber)).ToString());
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

    /// <summary>Remove diacritics so that e.g. "fjällmaraton" becomes "fjallmaraton".</summary>
    private static string StripDiacritics(string text)
    {
        // First replace characters that don't decompose under NFD (stroke/ligature letters).
        var sb = new System.Text.StringBuilder(text.Length);
        foreach (var c in text)
        {
            var mapped = c switch
            {
                'ø' or 'ö' => "o", 'Ø' or 'Ö' => "O",
                'æ' => "ae", 'Æ' => "AE",
                'ð' => "d", 'Ð' => "D",
                'þ' => "th", 'Þ' => "TH",
                'đ' => "d", 'Đ' => "D",
                'ł' => "l", 'Ł' => "L",
                _ => null
            };
            if (mapped is not null) sb.Append(mapped);
            else sb.Append(c);
        }
        // Then strip combining marks (handles ä→a, é→e, etc.).
        var normalized = sb.ToString().Normalize(System.Text.NormalizationForm.FormD);
        sb.Clear();
        foreach (var c in normalized)
            if (CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark)
                sb.Append(c);
        return sb.ToString().Normalize(System.Text.NormalizationForm.FormC);
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

        // Cap input size — dates are in the first portion of the page; large pages cause regex slowdowns.
        const int MaxChars = 150_000;
        if (html.Length > MaxChars)
            html = html[..MaxChars];

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
        // Strip <head>, script/style blocks, and HTML comments so non-visible dates aren't matched.
        var cleanHtml = HeadSectionRegex().Replace(html, " ");
        cleanHtml = ScriptStyleRegex().Replace(cleanHtml, " ");
        // Strip all HTML tags so dates inside attributes (src, content, href) aren't matched.
        var visibleText = HtmlTagRegex().Replace(cleanHtml, " ");
        var candidates = new Dictionary<string, int>(StringComparer.Ordinal); // normalized date → score
        foreach (Match m in VisibleDateRegex().Matches(visibleText))
        {
            var normalized = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(m.Value.Trim());
            if (normalized is null) continue;

            int score = 1; // base occurrence score

            // Check surrounding context in the HTML (with tags) for heading tags or large font-size.
            var htmlIdx = cleanHtml.IndexOf(m.Value, StringComparison.OrdinalIgnoreCase);
            var contextStart = htmlIdx >= 0 ? Math.Max(0, htmlIdx - 200) : 0;
            var contextLen = htmlIdx >= 0
                ? Math.Min(cleanHtml.Length - contextStart, m.Value.Length + 400)
                : 0;
            var context = htmlIdx >= 0 ? cleanHtml.AsSpan(contextStart, contextLen) : ReadOnlySpan<char>.Empty;

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

        // Fallback: capture loose visible date text even when the primary month-pattern regex misses it.
        foreach (Match m in LooseVisibleDateRegex().Matches(visibleText))
        {
            var normalized = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(m.Value.Trim());
            if (normalized is not null)
                return normalized;
        }

        return null;
    }

    /// <summary>
    /// Extracts absolute URLs for external CSS stylesheets referenced via &lt;link rel="stylesheet"&gt;.
    /// </summary>
    public static List<Uri> ExtractStylesheetUrls(string html, Uri pageUrl)
    {
        var urls = new List<Uri>();
        foreach (Match m in StylesheetLinkRegex().Matches(html))
        {
            var href = m.Groups["href"].Value.Trim();
            if (!string.IsNullOrEmpty(href) && Uri.TryCreate(pageUrl, href, out var uri)
                && uri.Scheme is "http" or "https")
                urls.Add(uri);
        }
        return urls;
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
                // Demote OG images that are likely logos/graphics rather than hero photos.
                var p = uri.AbsolutePath;
                if (p.Contains("logo", StringComparison.OrdinalIgnoreCase)
                    || p.Contains("/elementor/thumbs/", StringComparison.OrdinalIgnoreCase)
                    || p.EndsWith(".png", StringComparison.OrdinalIgnoreCase)
                    || p.EndsWith(".svg", StringComparison.OrdinalIgnoreCase))
                {
                    ogCandidate ??= uri;
                    continue;
                }
                return uri;
            }
        }

        // 2. Find ALL image URLs anywhere in the HTML (img src, background-image, data-settings JSON, style attrs, etc.)
        //    Decode HTML entities and JSON slash escaping so all URL forms become plain https://...
        var decoded = html.Replace("&quot;", "\"").Replace("&amp;", "&").Replace("&#039;", "'").Replace("\\/", "/");
        Uri? best = null;
        var bestScore = int.MinValue;

        void Consider(Uri uri, double position, int bonus = 0)
        {
            if (IsTrackingPixel(uri)) return;
            var score = ScoreImageUrl(uri) + (int)Math.Round(2 * (1 - position)) + bonus;
            if (score > bestScore) { bestScore = score; best = uri; }
        }

        // 2a. background-image: url(...) — can contain relative URLs, so resolve against page.
        foreach (Match m in CssBackgroundImageRegex().Matches(decoded))
        {
            var rawUrl = m.Groups["url"].Value.Trim();
            if (!Uri.TryCreate(pageUrl, rawUrl, out var uri)
                || uri.Scheme is not ("http" or "https"))
                continue;
            var ext = uri.AbsolutePath.AsSpan();
            var dot = ext.LastIndexOf('.');
            if (dot < 0) continue;
            ext = ext.Slice(dot);
            if (!ext.Equals(".jpg", StringComparison.OrdinalIgnoreCase)
                && !ext.Equals(".jpeg", StringComparison.OrdinalIgnoreCase)
                && !ext.Equals(".webp", StringComparison.OrdinalIgnoreCase)
                && !ext.Equals(".png", StringComparison.OrdinalIgnoreCase)
                && !ext.Equals(".gif", StringComparison.OrdinalIgnoreCase)
                && !ext.Equals(".svg", StringComparison.OrdinalIgnoreCase))
                continue;
            var position = (double)m.Index / Math.Max(decoded.Length, 1);
            // Bonus: background-image is usually a hero/banner, worth boosting.
            Consider(uri, position, bonus: 3);
        }

        // 2b. Absolute image URLs anywhere in the HTML (img src, JSON, style attrs, etc.)
        foreach (Match m in AnyImageUrlRegex().Matches(decoded))
        {
            var rawUrl = UnescapeJsonSlash(m.Groups["url"].Value);
            if (!Uri.TryCreate(pageUrl, rawUrl, out var uri)
                || uri.Scheme is not ("http" or "https"))
                continue;
            var position = (double)m.Index / Math.Max(decoded.Length, 1);
            Consider(uri, position);
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

    private static int ScoreImageUrl(Uri uri)
    {
        var path = uri.AbsolutePath.ToLowerInvariant();

        // Hard reject known non-content paths.
        if (path.Contains("icon") || path.Contains("avatar") || path.Contains("emoji")
            || path.Contains("spinner") || path.Contains("loading")
            || path.Contains("map") || path.Contains("karta")
            || path.Contains("download") || path.Contains("upload"))
            return -100;

        var score = 0;

        // Penalize logos.
        if (path.Contains("logo"))
            score -= 10;

        // Penalize Elementor thumbnail crops and WordPress thumbnail sizes.
        if (path.Contains("/elementor/thumbs/") || path.Contains("-150x150")
            || path.Contains("-100x100") || path.Contains("-300x"))
            score -= 5;

        // Photographic formats are almost always real content; png/gif/svg are often logos/graphics.
        if (path.EndsWith(".jpg") || path.EndsWith(".jpeg") || path.EndsWith(".webp")
            || path.EndsWith(".jpg.webp") || path.EndsWith(".jpeg.webp"))
            score += 5;
        else if (path.EndsWith(".png"))
            score -= 3;
        else if (path.EndsWith(".gif") || path.EndsWith(".svg") || path.EndsWith(".ico"))
            score -= 10;

        // Dimensions embedded in path (common WordPress/Elementor pattern: image-1024x768.jpg).
        var dimMatch = PathDimensionRegex().Match(path);
        if (dimMatch.Success
            && int.TryParse(dimMatch.Groups["w"].Value, out var w)
            && int.TryParse(dimMatch.Groups["h"].Value, out var h))
        {
            var pixels = w * h;
            if (pixels < 50 * 50) return -100; // tiny
            score += pixels > 500_000 ? 4 : pixels > 100_000 ? 2 : 0;
        }

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

    [GeneratedRegex(@"<tr[^>]*>\s*<td[^>]*><b>\s*Web page:\s*</b>\s*</td>\s*<td[^>]*>(.*?)</td>\s*</tr>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex DuvWebPageRowRegex();

    // Strips all HTML tags so anchor inner content can be inspected as plain text.
    [GeneratedRegex(@"<[^>]+>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex HtmlTagRegex();

    // Removes <script>...</script>, <style>...</style> blocks, and HTML comments.
    // Uses [^<]* with <(?!/) lookahead to avoid catastrophic backtracking on large pages.
    [GeneratedRegex(@"<(?:script|style)\b[^>]*>(?:[^<]|<(?!/(?:script|style)>))*</(?:script|style)>|<!--(?:[^-]|-(?!->))*-->", RegexOptions.IgnoreCase)]
    private static partial Regex ScriptStyleRegex();

    // Removes the <head>...</head> section (meta tags, title, link tags, etc.).
    [GeneratedRegex(@"<head\b[^>]*>(?:[^<]|<(?!/head>))*</head>", RegexOptions.IgnoreCase)]
    private static partial Regex HeadSectionRegex();

    // Extracts WxH dimensions embedded in image file paths (e.g. image-1024x768.jpg, photo_800x600.webp).
    [GeneratedRegex(@"-(?<w>\d{2,4})x(?<h>\d{2,4})\.", RegexOptions.IgnoreCase)]
    private static partial Regex PathDimensionRegex();

    // Matches <link rel="stylesheet" href="..."> tags.
    [GeneratedRegex(@"<link\b[^>]*\brel\s*=\s*[""']stylesheet[""'][^>]*\bhref\s*=\s*[""'](?<href>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex StylesheetLinkRegex();

    // Broad regex: finds any image URL (absolute) anywhere in HTML — img src, background-image, JSON, style attrs, etc.
    // Matches common image extensions: jpg, jpeg, webp, png, gif, svg.
    [GeneratedRegex(@"(?<url>https?://[^\s""'<>\)]+\.(?:jpe?g|webp|png|gif|svg)(?:\.webp)?)", RegexOptions.IgnoreCase)]
    private static partial Regex AnyImageUrlRegex();

    // Matches background-image: url('...') in CSS/style attrs — captures the URL (absolute or relative).
    // Uses [^;]{0,80} to avoid crossing HTML attribute boundaries on entity-decoded pages.
    [GeneratedRegex(@"background(?:-image)?\s*:[^;]{0,80}url\(\s*[""']?(?<url>[^""')\s]+)[""']?\s*\)", RegexOptions.IgnoreCase)]
    private static partial Regex CssBackgroundImageRegex();

    // Matches <meta property="og:image" content="..."> and twitter:image variants.
    [GeneratedRegex(@"<meta\b[^>]*(?:property|name)\s*=\s*[""'](?:og:image|twitter:image)[""'][^>]*content\s*=\s*[""'](?<url>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex OgImageRegex();

    // Matches <img ...> tags.
    [GeneratedRegex(@"<img\b[^>]*>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex ImgRegex();

    // Extracts the src attribute from an <img> tag.
    [GeneratedRegex(@"src\s*=\s*[""'](?<src>[^""']+)[""']", RegexOptions.IgnoreCase)]
    private static partial Regex ImgSrcRegex();

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
    [GeneratedRegex(@"\b\d{1,2}\.?\s+(?:[A-Za-z]+\s+\d{4})\b|\b[A-Za-z]+\s+\d{1,2},?\s+\d{4}\b|\b\d{1,2}[/\.]\d{1,2}[/\.]\d{4}\b|\b\d{4}-\d{2}-\d{2}\b", RegexOptions.IgnoreCase)]
    private static partial Regex LooseVisibleDateRegex();
    // Matches distance patterns in URL paths: "80-km", "80km", "100k", "100M", "42.195-km", "10-miles".
    [GeneratedRegex(@"(?:^|[/\-_])(?<num>\d+(?:\.\d+)?)\s*-?\s*(?<unit>km|k|(?-i:M)|mi(?:les?)?|miles?)(?=[/\-_?#]|$)", RegexOptions.IgnoreCase)]
    private static partial Regex UrlDistanceRegex();

    // Matches distance in link text: "27K", "100 km", "45K", "100M", "10 miles".
    [GeneratedRegex(@"\b\d+(?:\.\d+)?\s*(?:km|k|(?-i:M)|mi(?:les?)?)\b", RegexOptions.IgnoreCase)]
    private static partial Regex LinkTextDistanceRegex();

    // Matches distance in visible page content with named groups: "27 km", "100K", "100M", "42.195 km", "10 miles".
    [GeneratedRegex(@"\b(?<num>\d+(?:\.\d+)?)\s*(?<unit>km|k|(?-i:M)|mi(?:les?)?)\b", RegexOptions.IgnoreCase)]
    private static partial Regex ContentDistanceRegex();

    // Matches inline font-size declarations like "font-size: 24px" or "font-size:1.5em".
    [GeneratedRegex(@"font-size\s*:\s*(?<size>\d+(?:\.\d+)?)\s*(?<unit>px|em|rem)", RegexOptions.IgnoreCase)]
    private static partial Regex FontSizeRegex();

    // Matches JSON-LD "name" field (Event, SportsEvent schemas).
    [GeneratedRegex(@"""name""\s*:\s*""(?<name>[^""]{3,100})""", RegexOptions.IgnoreCase)]
    private static partial Regex JsonLdNameRegex();

    // Matches <script type="application/ld+json">...</script> blocks.
    [GeneratedRegex(@"<script[^>]*type\s*=\s*[""']application/ld\+json[""'][^>]*>(?<json>.*?)</script>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex LdJsonBlockRegex();

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

    // Matches JSON-style unicode escapes like \u00e4.
    [GeneratedRegex(@"\\u([0-9a-fA-F]{4})")]
    private static partial Regex JsonUnicodeEscapeRegex();

    // Matches elevation gain keywords followed by a number, or a number followed by elevation keywords.
    // Patterns: "elevation gain: 1200 m", "Elevation Gain ↙ 900 m ↘ 1050 m", "höjdmeter: 1200", "D+ 1200m".
    // Allows up to 40 chars gap between keyword and number (tags are stripped first).
    private static readonly string[] ElevationKeywords =
        ["elevation gain", "ascent", "dénivelé positif", "dénivelé", "denivelé", "denivele",
         "aufstieg", "höjdmeter", "höhenmeter", "hohenmeter", "stigning", "totalstigning", "d+"];

    [GeneratedRegex(@"\b([+-]?(?:\d[\d\s]{0,6}\d|\d{1,5}))(?:\s*(?:m\b|meter\b|hm\b))?", RegexOptions.IgnoreCase)]
    private static partial Regex ElevationNumberRegex();

    /// <summary>
    /// Extracts elevation gain (in metres) from visible text in an HTML page.
    /// Finds the largest plausible value (1–99999 m) near an elevation keyword.
    /// </summary>
    public static double? ExtractElevationGain(string? html)
    {
        if (string.IsNullOrWhiteSpace(html)) return null;

        var text = HtmlTagRegex().Replace(html, " ");

        var pageDistanceKm = ExtractPageDistanceKm(text);
        double? best = null;
        foreach (var keyword in ElevationKeywords)
        {
            var kwIdx = text.IndexOf(keyword, StringComparison.OrdinalIgnoreCase);
            if (kwIdx < 0) continue;

            var lineStart = text.LastIndexOfAny(['\r', '\n'], kwIdx);
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            var lineEnd = text.IndexOfAny(['\r', '\n'], kwIdx + keyword.Length);
            if (lineEnd < 0) lineEnd = text.Length;
            var line = text[lineStart..lineEnd];

            foreach (Match match in ElevationNumberRegex().Matches(line))
            {
                var raw = match.Groups[1].Value.Replace(" ", "");
                if (double.TryParse(raw, NumberStyles.Float,
                    CultureInfo.InvariantCulture, out var metres)
                    && metres >= 1 && metres <= 99999)
                {
                    if (pageDistanceKm is not null && metres > pageDistanceKm.Value * 500)
                        continue;

                    // Take the largest value on the same line as the keyword.
                    if (!best.HasValue || metres > best.Value)
                        best = metres;
                }
            }
        }
        return best;
    }

    /// <summary>
    /// Extracts a price from visible HTML text.
    /// Looks for patterns like "500 kr", "€50", "SEK 300", "$120", "300 NOK", "1 200 SEK".
    /// Returns (amount, currency) or null.
    /// </summary>
    public static PriceInfo? ExtractPrice(string? html, Uri? pageUrl = null)
    {
        if (string.IsNullOrWhiteSpace(html)) return null;

        var tld = pageUrl?.Host.Split('.').LastOrDefault()?.ToLowerInvariant();

        // Strip non-visible content.
        var clean = HeadSectionRegex().Replace(html, " ");
        clean = ScriptStyleRegex().Replace(clean, " ");
        var text = HtmlTagRegex().Replace(clean, " ");

        // 1. JSON-LD "offers" → "price" + "priceCurrency".
        var priceMatch = JsonLdPriceRegex().Match(html);
        if (priceMatch.Success)
        {
            var raw = priceMatch.Groups["price"].Value.Replace(" ", "");
            var curr = priceMatch.Groups["currency"].Value.Trim().ToUpperInvariant();
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var p) && p > 0 && p < 100_000)
            {
                if (string.IsNullOrEmpty(curr)) curr = KrCurrencyForTld(tld);
                return new PriceInfo(p.ToString(CultureInfo.InvariantCulture), NormalizeCurrency(curr, tld));
            }
        }

        // 2. Visible text patterns: prefer explicit ranges.
        foreach (Match m in PriceRangeRegex().Matches(text))
        {
            var raw1 = m.Groups["num1"].Value.Replace(" ", "").Replace("\u00a0", "");
            var raw2 = m.Groups["num2"].Value.Replace(" ", "").Replace("\u00a0", "");
            if (!int.TryParse(raw1, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount1)
                || !int.TryParse(raw2, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount2))
            {
                continue;
            }

            if (amount1 < 10 || amount1 > 100_000 || amount2 < 10 || amount2 > 100_000)
                continue;

            var currencyToken = m.Groups["pre1"].Value.Trim();
            if (string.IsNullOrEmpty(currencyToken)) currencyToken = m.Groups["post1"].Value.Trim();
            if (string.IsNullOrEmpty(currencyToken)) currencyToken = m.Groups["pre2"].Value.Trim();
            if (string.IsNullOrEmpty(currencyToken)) currencyToken = m.Groups["post2"].Value.Trim();

            var currency = ResolveCurrency(currencyToken, currencyToken, tld);
            if (currency is null) continue;

            var min = Math.Min(amount1, amount2);
            var max = Math.Max(amount1, amount2);
            return new PriceInfo($"{min}-{max}", NormalizeCurrency(currency, tld));
        }

        // 3. Single price values.
        var best = (Amount: string.Empty, Currency: string.Empty);
        var bestScore = 0;

        foreach (Match m in PriceRegex().Matches(text))
        {
            var prefix = m.Groups["pre"].Value.Trim();
            var numRaw = m.Groups["num"].Value.Replace(" ", "").Replace("\u00a0", "");
            var suffix = m.Groups["post"].Value.Trim();

            if (!int.TryParse(numRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount))
                continue;
            if (amount < 10 || amount > 100_000) continue;

            var currency = ResolveCurrency(prefix, suffix, tld);
            if (currency is null) continue;

            // Prefer prices near registration keywords; among those, take the max amount.
            var idx = m.Index;
            var ctxStart = Math.Max(0, idx - 120);
            var ctxLen = Math.Min(text.Length - ctxStart, m.Length + 240);
            var ctx = text.AsSpan(ctxStart, ctxLen);
            var score = 1;
            if (HasPriceContext(ctx)) score += 5;
            if (amount >= 100) score += 1; // more likely a real entry fee

            if (score > bestScore || (score == bestScore && amount > int.Parse(best.Amount, CultureInfo.InvariantCulture)))
            {
                best = (amount.ToString(CultureInfo.InvariantCulture), NormalizeCurrency(currency, tld));
                bestScore = score;
            }
        }

        return bestScore > 0 ? new PriceInfo(best.Amount, best.Currency) : null;
    }

    private static bool HasPriceContext(ReadOnlySpan<char> ctx)
    {
        return ctx.Contains("pris", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("price", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("fee", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("avgift", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("anmälan", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("anmälning", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("registration", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("inscription", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("tarif", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("startavgift", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("p\u00e5melding", StringComparison.OrdinalIgnoreCase) // påmelding
            || ctx.Contains("deltaker", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("entry", StringComparison.OrdinalIgnoreCase);
    }

    private static string? ResolveCurrency(string prefix, string suffix, string? tld)
    {
        // Check prefix first (€, $, £, CHF, SEK, NOK, DKK, EUR, USD, GBP, EURO)
        var token = prefix.Length > 0 ? prefix : suffix;
        if (string.IsNullOrWhiteSpace(token)) return null;
        token = token.Trim('.', ',', ':').Trim();
        return token.ToUpperInvariant() switch
        {
            "KR" or "KR." => KrCurrencyForTld(tld),
            "SEK" => "SEK",
            "NOK" => "NOK",
            "DKK" => "DKK",
            "€" or "EUR" or "EURO" or "EUROS" => "EUR",
            "$" or "USD" => "USD",
            "£" or "GBP" => "GBP",
            "CHF" => "CHF",
            "ISK" => "ISK",
            _ => null
        };
    }

    private static string KrCurrencyForTld(string? tld) => tld switch
    {
        "no" => "NOK",
        "dk" => "DKK",
        "is" => "ISK",
        _ => "SEK", // .se, .com, and everything else
    };

    private static string NormalizeCurrency(string currency, string? tld = null) => currency.ToUpperInvariant() switch
    {
        "KR" or "KR." => KrCurrencyForTld(tld),
        "€" => "EUR",
        "$" => "USD",
        "£" => "GBP",
        "EURO" or "EUROS" => "EUR",
        _ => currency.ToUpperInvariant()
    };

    // Matches explicit price ranges like "60€ - 90€", "60 - 90 EUR", "€60 to €90".
    [GeneratedRegex(@"(?<pre1>[€$£]|(?:SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS|kr\.?)\s?)?(?<num1>\d[\d\s\u00a0]{0,6}\d|\d{1,5})(?:,-)?\s*(?<post1>kr\.?|SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS)?\s*(?:[-–—]|to|till|until)\s*(?<pre2>[€$£]|(?:SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS|kr\.?)\s?)?(?<num2>\d[\d\s\u00a0]{0,6}\d|\d{1,5})(?:,-)?\s*(?<post2>kr\.?|SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS)?", RegexOptions.IgnoreCase)]
    private static partial Regex PriceRangeRegex();

    // Matches price amounts with optional currency prefix/suffix.
    // "500 kr", "€50", "SEK 300", "1 200 NOK", "kr 500", "$120", "600,- NOK".
    [GeneratedRegex(@"(?<pre>[€$£]|(?:SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS|kr\.?)\s?)?(?<num>\d[\d\s\u00a0]{0,6}\d|\d{1,5})(?:,-)?\s*(?<post>kr\.?|SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS)?", RegexOptions.IgnoreCase)]
    private static partial Regex PriceRegex();

    // Matches JSON-LD "price" and "priceCurrency" in offers.
    [GeneratedRegex(@"""price""\s*:\s*""?(?<price>\d[\d\s]{0,6}\d|\d{1,5})""?.*?""priceCurrency""\s*:\s*""(?<currency>[A-Z]{3})""", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex JsonLdPriceRegex();

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
