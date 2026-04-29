using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Backend.Scrapers;
using Shared.Services;

namespace Backend;

public static partial class RaceHtmlScraper
{
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
        return host.Split(new[] { '.', '-' }, StringSplitOptions.RemoveEmptyEntries)
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

        var decoded = html.Replace("&quot;", "\"").Replace("&amp;", "&").Replace("&#039;", "'").Replace("\\/", "/");
        var ogCandidate = ExtractOgImageCandidate(html, pageUrl);
        if (ogCandidate is { Score: >= 0 } preferredOgCandidate)
            return preferredOgCandidate.Uri;

        return EnumerateProminentImageCandidates(decoded, pageUrl)
            .OrderByDescending(candidate => candidate.Score)
            .Select(candidate => candidate.Uri)
            .FirstOrDefault()
            ?? ogCandidate?.Uri;
    }

    private static IEnumerable<ScoredImageCandidate> EnumerateProminentImageCandidates(string html, Uri pageUrl)
    {
        foreach (var candidate in ExtractBackgroundImageCandidates(html, pageUrl))
            yield return candidate;

        foreach (var candidate in ExtractImgTagCandidates(html, pageUrl))
            yield return candidate;

        foreach (var candidate in ExtractAnyImageUrlCandidates(html, pageUrl))
            yield return candidate;
    }

    private static ScoredImageCandidate? ExtractOgImageCandidate(string html, Uri pageUrl)
    {
        foreach (Match m in OgImageRegex().Matches(html))
        {
            var content = m.Groups["url"].Value.Trim();
            if (!TryCreateScoredCandidate(pageUrl, UnescapeJsonSlash(content), position: 0, bonus: 0, out var candidate))
                continue;

            var path = candidate.Uri.AbsolutePath;
            if (path.Contains("logo", StringComparison.OrdinalIgnoreCase)
                || path.Contains("/elementor/thumbs/", StringComparison.OrdinalIgnoreCase)
                || path.EndsWith(".png", StringComparison.OrdinalIgnoreCase)
                || path.EndsWith(".svg", StringComparison.OrdinalIgnoreCase))
            {
                return candidate with { Score = -3 };
            }

            return candidate;
        }

        return null;
    }

    private static IEnumerable<ScoredImageCandidate> ExtractBackgroundImageCandidates(string html, Uri pageUrl)
    {
        foreach (Match m in CssBackgroundImageRegex().Matches(html))
        {
            var rawUrl = m.Groups["url"].Value.Trim();
            if (!HasSupportedImageExtension(rawUrl, pageUrl))
                continue;

            var position = (double)m.Index / Math.Max(html.Length, 1);
            var bonus = 5 + GetBackgroundContextBonus(html, m.Index) + GetElementorContextBonus(GetBackgroundSelectorContext(html, m.Index));
            if (TryCreateScoredCandidate(pageUrl, rawUrl, position, bonus, out var candidate))
                yield return candidate;
        }
    }

    private static IEnumerable<ScoredImageCandidate> ExtractImgTagCandidates(string html, Uri pageUrl)
    {
        foreach (Match m in ImgRegex().Matches(html))
        {
            var tag = m.Value;
            var imageUrl = GetImgCandidateUrl(tag);
            if (string.IsNullOrWhiteSpace(imageUrl))
                continue;

            var classValue = ImgClassRegex().Match(tag).Groups["class"].Value;
            var bonus = (HasBackgroundClassHint(classValue) ? 4 : 0)
                + GetImageDimensionScoreAdjustment(tag)
                + GetElementorContextBonus(tag + " " + GetSurroundingContext(html, m.Index, 400, 0));
            var position = (double)m.Index / Math.Max(html.Length, 1);
            if (TryCreateScoredCandidate(pageUrl, imageUrl, position, bonus, out var candidate))
                yield return candidate;
        }
    }

    private static IEnumerable<ScoredImageCandidate> ExtractAnyImageUrlCandidates(string html, Uri pageUrl)
    {
        foreach (Match m in AnyImageUrlRegex().Matches(html))
        {
            if (IsInsideImgTag(html, m.Index))
                continue;

            var position = (double)m.Index / Math.Max(html.Length, 1);
            var bonus = GetGenericImageContextBonus(html, m.Index);
            if (TryCreateScoredCandidate(pageUrl, UnescapeJsonSlash(m.Groups["url"].Value), position, bonus, out var candidate))
                yield return candidate;
        }
    }

    private static bool TryCreateScoredCandidate(Uri pageUrl, string rawUrl, double position, int bonus, out ScoredImageCandidate candidate)
    {
        candidate = default;
        if (!Uri.TryCreate(pageUrl, rawUrl, out var uri)
            || uri.Scheme is not ("http" or "https")
            || IsTrackingPixel(uri))
        {
            return false;
        }

        var score = ScoreImageUrl(uri) + (int)Math.Round(2 * (1 - position)) + bonus;
        candidate = new ScoredImageCandidate(uri, score);
        return true;
    }

    private static bool HasSupportedImageExtension(string rawUrl, Uri pageUrl)
    {
        if (!Uri.TryCreate(pageUrl, rawUrl, out var uri))
            return false;

        var extension = Path.GetExtension(uri.AbsolutePath);
        return extension.Equals(".jpg", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".jpeg", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".webp", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".png", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".gif", StringComparison.OrdinalIgnoreCase)
            || extension.Equals(".svg", StringComparison.OrdinalIgnoreCase);
    }

    private readonly record struct ScoredImageCandidate(Uri Uri, int Score);

    private static string? GetImgCandidateUrl(string imgTag)
    {
        foreach (var rawValue in new[]
        {
            ImgSrcRegex().Match(imgTag).Groups["src"].Value,
            ImgDataSrcRegex().Match(imgTag).Groups["src"].Value,
            ImgDataFullImageRegex().Match(imgTag).Groups["src"].Value,
            ImgDataLightImageRegex().Match(imgTag).Groups["src"].Value,
        })
        {
            if (string.IsNullOrWhiteSpace(rawValue) || rawValue.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
                continue;

            return UnescapeJsonSlash(rawValue);
        }

        return null;
    }

    private static bool HasBackgroundClassHint(string classValue)
    {
        if (string.IsNullOrWhiteSpace(classValue))
            return false;

        foreach (var token in classValue.Split([' ', '\t', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (token.Equals("bg", StringComparison.OrdinalIgnoreCase)
                || token.Contains("background", StringComparison.OrdinalIgnoreCase)
                || token.StartsWith("bg-", StringComparison.OrdinalIgnoreCase)
                || token.EndsWith("-bg", StringComparison.OrdinalIgnoreCase)
                || token.Contains("_bg", StringComparison.OrdinalIgnoreCase)
                || token.Contains("bg_", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static int GetBackgroundContextBonus(string html, int matchIndex)
    {
        return GetClassBasedBackgroundBonus(html, matchIndex) + GetHeroContextBonus(GetBackgroundSelectorContext(html, matchIndex));
    }

    private static int GetGenericImageContextBonus(string html, int matchIndex)
    {
        var context = GetSurroundingContext(html, matchIndex, 240, 240);
        var bonus = GetHeroContextBonus(context) + GetElementorContextBonus(context);

        if (context.Contains("background_slideshow_gallery", StringComparison.OrdinalIgnoreCase)
            || context.Contains("background_background\":\"slideshow", StringComparison.OrdinalIgnoreCase)
            || context.Contains("elementor-background-slideshow", StringComparison.OrdinalIgnoreCase))
        {
            bonus += 10;
        }

        return bonus;
    }

    private static int GetClassBasedBackgroundBonus(string html, int matchIndex)
    {
        var tagContext = TryGetContainingTag(html, matchIndex);
        var classValue = tagContext is not null
            ? ImgClassRegex().Match(tagContext).Groups["class"].Value
            : null;

        return HasBackgroundClassHint(classValue ?? string.Empty) ? 4 : 0;
    }

    private static int GetElementorContextBonus(string context)
    {
        if (string.IsNullOrWhiteSpace(context))
            return 0;

        var bonus = 0;
        if (context.Contains("elementor", StringComparison.OrdinalIgnoreCase))
            bonus += 3;

        if (context.Contains("elementor-widget-image", StringComparison.OrdinalIgnoreCase)
            || context.Contains("data-elementor", StringComparison.OrdinalIgnoreCase)
            || context.Contains("elementor-section", StringComparison.OrdinalIgnoreCase))
        {
            bonus += 2;
        }

        return bonus;
    }

    private static int GetHeroContextBonus(string context)
    {
        if (context.Contains("kb-bg-slide", StringComparison.OrdinalIgnoreCase)
            || context.Contains("kt-row-has-bg", StringComparison.OrdinalIgnoreCase)
            || context.Contains("kb-row-layout-wrap", StringComparison.OrdinalIgnoreCase)
            || context.Contains("elementor-background-slideshow", StringComparison.OrdinalIgnoreCase)
            || context.Contains("hero", StringComparison.OrdinalIgnoreCase)
            || context.Contains("banner", StringComparison.OrdinalIgnoreCase))
        {
            return 8;
        }

        return 0;
    }

    private static string? TryGetContainingTag(string html, int matchIndex)
    {
        var tagStart = html.LastIndexOf('<', matchIndex);
        if (tagStart < 0)
            return null;

        var tagEnd = html.IndexOf('>', matchIndex);
        if (tagEnd < 0 || tagEnd <= tagStart)
            return null;

        var nestedTagEnd = html.LastIndexOf('>', matchIndex);
        if (nestedTagEnd > tagStart)
            return null;

        return html[tagStart..(tagEnd + 1)];
    }

    private static string GetBackgroundSelectorContext(string html, int matchIndex)
    {
        var contextStart = Math.Max(0, matchIndex - 200);
        var context = html[contextStart..matchIndex];

        var lastBrace = context.LastIndexOf('{');
        if (lastBrace >= 0)
            context = context[..lastBrace];

        var lastCloseBrace = context.LastIndexOf('}');
        if (lastCloseBrace >= 0 && lastCloseBrace < context.Length - 1)
            context = context[(lastCloseBrace + 1)..];

        return context;
    }

    private static string GetSurroundingContext(string html, int matchIndex, int beforeChars, int afterChars)
    {
        var start = Math.Max(0, matchIndex - beforeChars);
        var length = Math.Min(html.Length - start, beforeChars + afterChars);
        return html.Substring(start, length);
    }

    private static int GetImageDimensionScoreAdjustment(string imgTag)
    {
        if (string.IsNullOrWhiteSpace(imgTag))
            return 0;

        var width = TryParseAttributeDimension(ImgWidthRegex().Match(imgTag).Groups["width"].Value);
        var height = TryParseAttributeDimension(ImgHeightRegex().Match(imgTag).Groups["height"].Value);

        if (!width.HasValue || !height.HasValue)
            return 0;

        var score = 0;

        if (width.Value > height.Value)
            score += 2;

        if (width.Value > 400 && height.Value > 200)
            score += 10;

        if (width.Value <= 48 && height.Value <= 48)
            score -= 25;

        if (width.Value * height.Value <= 4096)
            score -= 15;

        return score;
    }

    private static bool IsInsideImgTag(string html, int matchIndex)
    {
        var tag = TryGetContainingTag(html, matchIndex);
        return tag?.StartsWith("<img", StringComparison.OrdinalIgnoreCase) == true;
    }

    private static int? TryParseAttributeDimension(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var digitsMatch = LeadingIntegerRegex().Match(value.Trim());
        if (!digitsMatch.Success)
            return null;

        return int.TryParse(digitsMatch.Groups["value"].Value, out var dimension) ? dimension : null;
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

            var imageUrl = GetImgCandidateUrl(tag);
            if (string.IsNullOrWhiteSpace(imageUrl))
                continue;
            if (Uri.TryCreate(pageUrl, imageUrl, out var uri)
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
        var host = uri.Host;
        var path = uri.AbsolutePath;
        return (host.Equals("www.facebook.com", StringComparison.OrdinalIgnoreCase)
            || host.Equals("facebook.com", StringComparison.OrdinalIgnoreCase))
               && path.Equals("/tr", StringComparison.OrdinalIgnoreCase)
            || host.Equals("www.google-analytics.com", StringComparison.OrdinalIgnoreCase)
            || host.Equals("google-analytics.com", StringComparison.OrdinalIgnoreCase)
            || host.Equals("region1.google-analytics.com", StringComparison.OrdinalIgnoreCase)
            || host.Equals("stats.g.doubleclick.net", StringComparison.OrdinalIgnoreCase)
            || host.Equals("www.googletagmanager.com", StringComparison.OrdinalIgnoreCase)
            || host.Equals("connect.facebook.net", StringComparison.OrdinalIgnoreCase)
            || host.Equals("pixel.wp.com", StringComparison.OrdinalIgnoreCase)
            || path.Contains("pixel", StringComparison.OrdinalIgnoreCase)
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
            || path.Contains("download"))
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

}
