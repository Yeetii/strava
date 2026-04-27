using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Backend.Scrapers;
using Shared.Services;

namespace Backend;

public static partial class RaceHtmlScraper
{
    /// <summary>
    /// Extracts the external event website URL from a BeTrail event page.
    /// Looks for anchors whose visible text is "Site web" / "Website", then returns the first valid HTTP(S) href.
    /// </summary>
    public static Uri? ExtractBeTrailEventWebPageUrl(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        // First, prefer the "Site web" row in the structured summary table.
        var siteRowMatch = BeTrailSiteWebRowRegex().Match(html);
        if (siteRowMatch.Success)
        {
            var rowHtml = siteRowMatch.Groups["content"].Value;
            foreach (Match anchorMatch in AnchorRegex().Matches(rowHtml))
            {
                var href = anchorMatch.Groups["href"].Value;
                if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)
                    && uri.Scheme is "http" or "https"
                    && !string.Equals(uri.Host, pageUrl.Host, StringComparison.OrdinalIgnoreCase))
                {
                    return uri;
                }
            }
        }

        foreach (Match anchorMatch in AnchorRegex().Matches(html))
        {
            var href = anchorMatch.Groups["href"].Value;
            var text = HtmlTagRegex().Replace(anchorMatch.Groups["text"].Value, " ").Trim();

            if (!text.Contains("site web", StringComparison.OrdinalIgnoreCase)
                && !text.Contains("website", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (Uri.TryCreate(pageUrl, UnescapeJsonSlash(href), out var uri)
                && uri.Scheme is "http" or "https"
                && !string.Equals(uri.Host, pageUrl.Host, StringComparison.OrdinalIgnoreCase))
            {
                return uri;
            }
        }

        return null;
    }

    /// <summary>
    /// Extracts the event start date from a BeTrail event page date block.
    /// For date ranges, returns the first date (start), normalized as YYYY-MM-DD.
    /// Supports French month abbreviations/full names (e.g. "sept.", "septembre").
    /// </summary>
    public static string? ExtractBeTrailEventDate(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        // Prefer the structured summary table "Date" row.
        var summaryDateRow = BeTrailDateRowRegex().Match(html);
        if (summaryDateRow.Success)
        {
            var rowText = HtmlTagRegex().Replace(summaryDateRow.Groups["content"].Value, " ");
            // Keep only the first date if range is provided (start date).
            var firstDate = VisibleDateRegex().Match(rowText);
            if (firstDate.Success)
            {
                var normalized = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(firstDate.Value);
                if (!string.IsNullOrWhiteSpace(normalized))
                    return normalized;
            }
        }

        foreach (Match dateBlock in BeTrailDateBlockRegex().Matches(html))
        {
            var day = dateBlock.Groups["day"].Value.Trim();
            var monthRaw = dateBlock.Groups["month"].Value.Trim();
            var year = dateBlock.Groups["year"].Value.Trim();

            if (string.IsNullOrWhiteSpace(day)
                || string.IsNullOrWhiteSpace(monthRaw)
                || string.IsNullOrWhiteSpace(year))
            {
                continue;
            }

            var month = NormalizeBeTrailMonth(monthRaw);
            if (month is null)
                continue;

            var candidate = $"{day} {month} {year}";
            var normalized = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(candidate);
            if (!string.IsNullOrWhiteSpace(normalized))
                return normalized;
        }

        return null;
    }

    /// <summary>
    /// Extracts BeTrail "parcours" distances from the structured summary table.
    /// Example cell text: "124 - 60 - 10 - ... - 5 km" -> "124 km, 60 km, 10 km, ...".
    /// </summary>
    public static string? ExtractBeTrailEventDistances(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var rowMatch = BeTrailParcoursRowRegex().Match(html);
        if (!rowMatch.Success)
            return null;

        var cellText = HtmlTagRegex().Replace(rowMatch.Groups["content"].Value, " ");
        cellText = System.Net.WebUtility.HtmlDecode(cellText);

        // Capture all numeric segments in the parcours row, preserving order.
        var distances = new List<string>();
        foreach (Match m in BeTrailDistanceNumberRegex().Matches(cellText))
        {
            if (!double.TryParse(m.Groups["num"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var km))
                continue;
            if (km is < 1 or > 500)
                continue;
            distances.Add(RaceScrapeDiscovery.FormatDistanceKm(km));
        }

        if (distances.Count == 0)
            return null;

        // De-duplicate while preserving order.
        var unique = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var d in distances)
        {
            if (seen.Add(d))
                unique.Add(d);
        }

        return unique.Count > 0 ? string.Join(", ", unique) : null;
    }

    /// <summary>
    /// Extracts distances from BeTrail sub-event cards (race-info-block-container).
    /// Example source: itemprop="name" content="124km - 4635D+ UTHC 125"
    /// </summary>
    public static string? ExtractBeTrailSubEventDistances(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var distances = new List<string>();

        foreach (Match m in BeTrailSubEventMetaNameRegex().Matches(html))
        {
            var content = m.Groups["content"].Value;
            var kmMatch = BeTrailKmRegex().Match(content);
            if (!kmMatch.Success)
                continue;

            if (!double.TryParse(kmMatch.Groups["km"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var km))
                continue;

            if (km is < 1 or > 500)
                continue;

            distances.Add(RaceScrapeDiscovery.FormatDistanceKm(km));
        }

        if (distances.Count == 0)
            return null;

        var unique = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var d in distances)
        {
            if (seen.Add(d))
                unique.Add(d);
        }

        return unique.Count > 0 ? string.Join(", ", unique) : null;
    }

    /// <summary>
    /// Extracts max elevation gain in meters from BeTrail sub-event cards.
    /// Example source: "4635 D+" in race-info-elevation or meta name.
    /// </summary>
    public static double? ExtractBeTrailSubEventElevationGain(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        double? max = null;

        foreach (Match m in BeTrailSubEventElevationRegex().Matches(html))
        {
            if (!double.TryParse(m.Groups["elev"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var elev))
                continue;

            if (elev is < 0 or > 20000)
                continue;

            if (!max.HasValue || elev > max.Value)
                max = elev;
        }

        return max;
    }

    /// <summary>
    /// Extracts race types from BeTrail category classes such as:
    /// class="race-info-category trail" -> "trail"
    /// </summary>
    public static string? ExtractBeTrailRaceType(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var tokens = new List<string>();
        foreach (Match m in BeTrailRaceCategoryClassRegex().Matches(html))
        {
            var cls = m.Groups["class"].Value;
            var parts = cls.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            foreach (var part in parts)
            {
                if (string.Equals(part, "race-info-category", StringComparison.OrdinalIgnoreCase))
                    continue;
                tokens.Add(part);
            }
        }

        if (tokens.Count == 0)
            return null;

        return RaceScrapeDiscovery.NormalizeRaceType(string.Join(", ", tokens));
    }

    /// <summary>
    /// Extracts one BeTrail sub-event per race info card so discovery can emit one record per race.
    /// </summary>
    public static IReadOnlyList<BeTrailSubEventInfo> ExtractBeTrailSubEvents(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var starts = BeTrailSubEventStartRegex().Matches(html);
        if (starts.Count == 0)
            return [];

        var results = new List<BeTrailSubEventInfo>(starts.Count);
        for (int i = 0; i < starts.Count; i++)
        {
            var start = starts[i].Index;
            var end = i + 1 < starts.Count ? starts[i + 1].Index : html.Length;
            var section = html[start..end];

            string? date = null;
            var dateMatch = BeTrailSubEventStartDateRegex().Match(section);
            if (dateMatch.Success)
                date = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(dateMatch.Groups["date"].Value.Trim())
                    ?? dateMatch.Groups["date"].Value.Trim();

            string? distance = null;
            var distanceMatch = BeTrailSubEventDistanceRegex().Match(section);
            if (distanceMatch.Success
                && double.TryParse(distanceMatch.Groups["km"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var km)
                && km is >= 1 and <= 500)
            {
                distance = RaceScrapeDiscovery.FormatDistanceKm(km);
            }
            else
            {
                // Fallback: meta itemprop name e.g. "124km - 4635D+ UTHC 125"
                var metaName = BeTrailSubEventMetaNameRegex().Match(section);
                if (metaName.Success)
                {
                    var kmMatch = BeTrailKmRegex().Match(metaName.Groups["content"].Value);
                    if (kmMatch.Success
                        && double.TryParse(kmMatch.Groups["km"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out km)
                        && km is >= 1 and <= 500)
                    {
                        distance = RaceScrapeDiscovery.FormatDistanceKm(km);
                    }
                }
            }

            double? elevationGain = null;
            var elevMatch = BeTrailSubEventElevationRegex().Match(section);
            if (elevMatch.Success
                && double.TryParse(elevMatch.Groups["elev"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var elev)
                && elev is >= 0 and <= 20000)
            {
                elevationGain = elev;
            }

            string? raceType = null;
            var categoryMatch = BeTrailRaceCategoryClassRegex().Match(section);
            if (categoryMatch.Success)
            {
                var cls = categoryMatch.Groups["class"].Value;
                var parts = cls.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                var tokens = parts
                    .Where(p => !string.Equals(p, "race-info-category", StringComparison.OrdinalIgnoreCase))
                    .ToList();
                if (tokens.Count > 0)
                    raceType = RaceScrapeDiscovery.NormalizeRaceType(string.Join(", ", tokens));
            }

            // Keep only meaningful sub-events.
            if (distance is null && elevationGain is null && raceType is null && date is null)
                continue;

            results.Add(new BeTrailSubEventInfo(distance, elevationGain, raceType, date));
        }

        return results;
    }

    private static string? NormalizeBeTrailMonth(string month)
    {
        if (string.IsNullOrWhiteSpace(month))
            return null;

        var key = month.Trim().TrimEnd('.').ToLowerInvariant();
        return key switch
        {
            // French abbreviations
            "janv" => "january",
            "fev" => "february",
            "fév" => "february",
            "mars" => "march",
            "avr" => "april",
            "mai" => "may",
            "juin" => "june",
            "juil" => "july",
            "aout" => "august",
            "août" => "august",
            "sept" => "september",
            "oct" => "october",
            "nov" => "november",
            "dec" => "december",
            "déc" => "december",
            // French full names
            "janvier" => "january",
            "fevrier" => "february",
            "février" => "february",
            "avril" => "april",
            "juillet" => "july",
            "septembre" => "september",
            "octobre" => "october",
            "novembre" => "november",
            "decembre" => "december",
            "décembre" => "december",
            _ => month
        };
    }

    // BeTrail structured summary row: <th>Site web</th><td>...<a href="...">...</a></td>
    [GeneratedRegex(@"<tr[^>]*>\s*<th[^>]*>\s*Site\s+web\s*</th>\s*<td[^>]*>(?<content>.*?)</td>\s*</tr>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailSiteWebRowRegex();

    // BeTrail structured summary row: <th>Date</th><td>...</td>
    [GeneratedRegex(@"<tr[^>]*>\s*<th[^>]*>\s*Date\s*</th>\s*<td[^>]*>(?<content>.*?)</td>\s*</tr>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailDateRowRegex();

    // BeTrail structured summary row where th contains "parcours" and td contains distance list.
    [GeneratedRegex(@"<tr[^>]*>\s*<th[^>]*>.*?\bparcours\b.*?</th>\s*<td[^>]*>(?<content>.*?)</td>\s*</tr>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailParcoursRowRegex();

    [GeneratedRegex(@"(?<num>\d+(?:\.\d+)?)")]
    private static partial Regex BeTrailDistanceNumberRegex();

    [GeneratedRegex(@"<meta[^>]*itemprop\s*=\s*[""']name[""'][^>]*content\s*=\s*[""'](?<content>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailSubEventMetaNameRegex();

    [GeneratedRegex(@"(?<km>\d+(?:\.\d+)?)\s*km\b", RegexOptions.IgnoreCase)]
    private static partial Regex BeTrailKmRegex();

    [GeneratedRegex(@"(?<elev>\d+(?:\.\d+)?)\s*D\+", RegexOptions.IgnoreCase)]
    private static partial Regex BeTrailSubEventElevationRegex();

    [GeneratedRegex(@"class\s*=\s*[""'](?<class>[^""']*\brace-info-category\b[^""']*)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailRaceCategoryClassRegex();

    [GeneratedRegex(@"<div[^>]*itemprop\s*=\s*[""']subEvent[""'][^>]*>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailSubEventStartRegex();

    [GeneratedRegex(@"<meta[^>]*itemprop\s*=\s*[""']startDate[""'][^>]*content\s*=\s*[""'](?<date>[^""']+)[""']", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailSubEventStartDateRegex();

    [GeneratedRegex(@"race-info-distance[^>]*>\s*<span[^>]*>\s*(?<km>\d+(?:\.\d+)?)\s*km", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailSubEventDistanceRegex();

    // BeTrail date card:
    // <span class="day">11</span><span class="month">sept.</span><span class="year">2026</span>
    // For ranges this appears twice; extraction logic picks the first match.
    [GeneratedRegex(@"<span[^>]*class\s*=\s*[""'][^""']*\bday\b[^""']*[""'][^>]*>(?<day>\d{1,2})</span>\s*<span[^>]*class\s*=\s*[""'][^""']*\bmonth\b[^""']*[""'][^>]*>(?<month>[^<]+)</span>\s*<span[^>]*class\s*=\s*[""'][^""']*\byear\b[^""']*[""'][^>]*>(?<year>\d{4})</span>", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex BeTrailDateBlockRegex();

}
