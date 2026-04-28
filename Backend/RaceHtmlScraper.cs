using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Backend.Scrapers;
using Shared.Services;

namespace Backend;

// Stateless helpers for extracting structured data from scraped HTML pages and
// for parsing HTTP API responses that are only consumed by ScrapeRaceWorker.
public static partial class RaceHtmlScraper
{
    /// <summary>
    /// Extracts the raw name candidate from HTML using structured sources in priority order:
    /// JSON-LD name, og:title, &lt;title&gt; tag (first segment), &lt;h1&gt; tag.
    /// Returns the raw string before any separator-splitting or filtering.
    /// </summary>
    public static string? ExtractNameCandidate(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        // 1. JSON-LD "name" field
        foreach (Match m in LdJsonBlockRegex().Matches(html))
        {
            try
            {
                using var doc = JsonDocument.Parse(m.Groups["json"].Value);
                if (doc.RootElement.ValueKind == JsonValueKind.Object && doc.RootElement.TryGetProperty("name", out var nameProp))
                {
                    var name = nameProp.GetString();
                    if (!string.IsNullOrWhiteSpace(name))
                        return name.Trim();
                }
            }
            catch { /* ignore parse errors */ }
        }

        // 2. og:title meta tag
        var ogTitleMatch = OgTitleRegex().Match(html);
        if (ogTitleMatch.Success)
        {
            var title = ogTitleMatch.Groups["title"].Value;
            if (!string.IsNullOrWhiteSpace(title))
                return title.Trim();
        }

        // 3. <title> tag (take first segment before separator)
        var titleTagMatch = TitleTagRegex().Match(html);
        if (titleTagMatch.Success)
        {
            var title = titleTagMatch.Groups["title"].Value;
            if (!string.IsNullOrWhiteSpace(title))
            {
                var parts = TitleSeparatorRegex().Split(title);
                return parts.Length > 0 ? parts[0].Trim() : title.Trim();
            }
        }

        // 4. <h1> tag
        foreach (Match m in HeadingRegex().Matches(html))
        {
            if (m.Groups["level"].Value == "1")
            {
                var h1 = HtmlTagRegex().Replace(m.Groups["text"].Value, " ");
                h1 = CollapseWhitespaceRegex().Replace(h1, " ").Trim();
                if (!string.IsNullOrWhiteSpace(h1))
                    return h1;
            }
        }

        return null;
    }

    /// <summary>
    /// Attempts to extract the event/race name from HTML using common patterns: JSON-LD, og:title, &lt;title&gt;, &lt;h1&gt;.
    /// When <paramref name="startPageName"/> is provided, separator splits prefer the segment least similar to it.
    /// </summary>
    public static string? ExtractName(string html, Uri pageUrl, string? startPageName = null)
    {
        var candidate = ExtractNameCandidate(html);
        if (string.IsNullOrWhiteSpace(candidate))
            return null;

        // If candidate contains a separator, pick the best segment.
        var separators = new[] { "|", "/", ",", "-" };
        var splitParts = candidate.Split(separators, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (splitParts.Length > 1)
        {
            // If a start page name is supplied, prefer the segment that overlaps least with it
            // (e.g. on "50K | My Race Event", pick "50K" rather than the event name).
            if (startPageName is not null)
            {
                var startWords = new HashSet<string>(
                    startPageName.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                        .Select(w => w.ToLowerInvariant()),
                    StringComparer.OrdinalIgnoreCase);
                int Overlap(string part) => part
                    .Split(' ', StringSplitOptions.RemoveEmptyEntries)
                    .Count(w => startWords.Contains(w.ToLowerInvariant()));
                var ordered = splitParts
                    .Where(p => !string.IsNullOrWhiteSpace(p))
                    .OrderBy(Overlap)
                    .ToList();
                if (ordered.Count > 0 && Overlap(ordered[0]) < Overlap(ordered[^1]))
                    return ordered[0].Trim();
                // tied — fall through to count method
            }

            var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (var part in splitParts)
            {
                if (string.IsNullOrWhiteSpace(part)) continue;
                int count = Regex.Matches(html, Regex.Escape(part), RegexOptions.IgnoreCase).Count;
                counts[part] = count;
            }
            var best = counts.OrderByDescending(kv => kv.Value).FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(best.Key))
                return best.Key.Trim();
            return splitParts[0].Trim();
        }
        return candidate.Trim();
    }
    public record PriceInfo(string Amount, string Currency);
    public record BeTrailSubEventInfo(string? Distance, double? ElevationGain, string? RaceType, string? Date);

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

        // Recognize 'Backyard' as a distance in the URL path
        if (path.Contains("backyard", StringComparison.OrdinalIgnoreCase))
            return "backyard";

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
            var km = RaceDistanceKm.TryParsePrimarySegmentKilometers(distance);
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

        // Cloud shared folders are often linked as "Downloads" / "download area" without "gpx" in the anchor text.
        foreach (Match match in AnchorRegex().Matches(html))
        {
            var href = match.Groups["href"].Value;
            if (href.StartsWith('#')) continue;
            if (!Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)) continue;
            if (uri.Scheme is not ("http" or "https")) continue;
            if (!IsGoogleDriveSharedFolder(uri) && !DropboxShareParser.IsDropboxSharedFolder(uri)) continue;
            if (seen.Add(uri.AbsoluteUri))
                results.Add(uri);
        }

        return results;
    }

    private static bool IsGoogleDriveSharedFolder(Uri uri) =>
        uri.Host.Equals("drive.google.com", StringComparison.OrdinalIgnoreCase)
        && uri.AbsolutePath.StartsWith("/drive/folders/", StringComparison.OrdinalIgnoreCase);

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
        + @"|sep(?:t(?:emb(?:er|re?))?)?|o[ck]t(?:ob(?:er|re))?|nov(?:emb(?:er|re))?|de[csz](?:emb(?:er|re))?|décembre";

    // Visible date patterns, multilingual:
    //   "14 September 2025", "14. september 2025", "lørdag den 24. mai 2025",
    //   "September 14, 2025", "14/09/2025", "14.09.2025", "2025-09-14",
    //   "Friday September 11th", "12:00, Friday September 11th" (yearless).
    [GeneratedRegex(
        @"\b\d{1,2}\.?\s+(?:" + MonthPattern + @")\s+\d{4}\b"               // 14 September 2025 / 14. mai 2025
        + @"|\b(?:" + MonthPattern + @")\s+\d{1,2},?\s+\d{4}\b"             // September 14, 2025
        + @"|\b\d{1,2}[/\.]\d{1,2}[/\.]\d{4}\b"                            // 14/09/2025 or 14.09.2025
        + @"|\b\d{4}-\d{2}-\d{2}\b"                                         // 2025-09-14
        + @"|\d{1,2}:\d{2}\s*,\s*(?:[A-Za-z]+\s+)?(?:" + MonthPattern + @")\s+\d{1,2}(?:st|nd|rd|th)?\b"  // 12:00, Friday September 11th
        + @"|\b(?:[A-Za-z]+\s+)?(?:" + MonthPattern + @")\s+\d{1,2}(?:st|nd|rd|th)\b",                    // Friday September 11th / September 11th
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

}
