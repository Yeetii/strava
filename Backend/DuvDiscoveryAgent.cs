using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using Shared.Services;

namespace Backend;

public static partial class DuvDiscoveryAgent
{
    public static readonly Uri CalendarUrl = new("https://statistik.d-u-v.org/calendar.php");

    public static IReadOnlyCollection<ScrapeJob> ParseCalendarPage(string html, Uri baseUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var jobs = new List<ScrapeJob>();
        foreach (Match rowMatch in DuvCalendarRowRegex().Matches(html))
        {
            var rowHtml = rowMatch.Groups[1].Value;
            var cellMatches = TableCellRegex().Matches(rowHtml);
            if (cellMatches.Count < 4)
                continue;

            var dateText = WebUtility.HtmlDecode(cellMatches[0].Groups[1].Value).Trim();
            var titleText = cellMatches[1].Groups[1].Value;
            var distanceText = WebUtility.HtmlDecode(cellMatches[2].Groups[1].Value).Trim();
            var locationText = WebUtility.HtmlDecode(cellMatches[3].Groups[1].Value).Trim();

            var linkMatch = DuvEventLinkRegex().Match(titleText);
            if (!linkMatch.Success)
                continue;

            var href = linkMatch.Groups["href"].Value;
            var name = WebUtility.HtmlDecode(linkMatch.Groups["text"].Value).Trim();
            if (string.IsNullOrWhiteSpace(name))
                continue;

            if (!Uri.TryCreate(baseUrl, href, out var detailUrl))
                continue;

            var distance = RaceScrapeDiscovery.ParseDistanceVerbose(distanceText);
            var date = RaceScrapeDiscovery.NormalizeDuvCalendarDateToYyyyMmDd(dateText);

            string? country = null;
            var location = locationText;
            var countryMatch = DuvCountryCodeRegex().Match(locationText);
            if (countryMatch.Success)
            {
                country = RaceScrapeDiscovery.NormalizeCountryToIso2(countryMatch.Groups["code"].Value) ?? countryMatch.Groups["code"].Value;
                location = locationText[..countryMatch.Index].Trim();
                if (location.EndsWith(",", StringComparison.Ordinal))
                    location = location[..^1].Trim();
            }

            string? eventId = null;
            var eventIdMatch = DuvEventIdRegex().Match(href);
            if (eventIdMatch.Success)
                eventId = eventIdMatch.Groups["id"].Value;

            jobs.Add(new ScrapeJob(
                WebsiteUrl: detailUrl,
                Name: name,
                ExternalIds: eventId is not null ? new Dictionary<string, string>(StringComparer.Ordinal) { ["duv"] = eventId } : null,
                Distance: distance,
                Date: date,
                Country: country,
                Location: string.IsNullOrWhiteSpace(location) ? null : location));
        }

        return jobs;
    }

    public static Uri? ExtractEventWebPageUrl(string html, Uri pageUrl)
        => RaceHtmlScraper.ExtractDuvEventWebPageUrl(html, pageUrl);

    public static ScrapeJob EnrichJobFromEventDetailHtml(ScrapeJob job, string html)
    {
        if (job.WebsiteUrl is null)
            return job;

        var externalSite = ExtractEventWebPageUrl(html, job.WebsiteUrl);
        var updated = job with { WebsiteUrl = externalSite ?? job.WebsiteUrl };

        var coords = ExtractStartPositionCoordinates(html);
        if (coords.HasValue)
            updated = updated with { Latitude = coords.Value.lat, Longitude = coords.Value.lng };

        var raceType = ExtractDuvDetailRowValue(html, "Race type");
        var elevationGain = ParseDuvElevationGain(ExtractDuvDetailRowValue(html, "Elevation gain/loss"));
        var description = ExtractDuvDetailRowValue(html, "Course description");
        var startFee = ExtractDuvDetailRowValue(html, "Entry fee");

        if (!string.IsNullOrWhiteSpace(raceType))
            updated = updated with { RaceType = raceType.Trim() };

        if (elevationGain.HasValue)
            updated = updated with { ElevationGain = elevationGain.Value };

        if (!string.IsNullOrWhiteSpace(description))
            updated = updated with { Description = description.Trim() };

        if (!string.IsNullOrWhiteSpace(startFee))
            updated = updated with { StartFee = startFee.Trim() };

        return updated;
    }

    public static async Task<ScrapeJob> EnrichJobAsync(HttpClient httpClient, ScrapeJob job, CancellationToken cancellationToken)
    {
        if (job.WebsiteUrl is null)
            return job;

        var html = await httpClient.GetStringAsync(job.WebsiteUrl, cancellationToken);
        var updated = EnrichJobFromEventDetailHtml(job, html);

        if (updated.Latitude is null || updated.Longitude is null)
        {
            var startPosUrl = ExtractStartPositionUrl(html, job.WebsiteUrl);
            if (startPosUrl is not null)
            {
                var startPosHtml = await httpClient.GetStringAsync(startPosUrl, cancellationToken);
                var coords = ExtractStartPositionCoordinates(startPosHtml);
                if (coords.HasValue)
                    updated = updated with { Latitude = coords.Value.lat, Longitude = coords.Value.lng };
            }
        }

        return updated;
    }

    public static Uri? ExtractStartPositionUrl(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var match = DuvStartPosLinkRegex().Match(html);
        if (!match.Success)
            return null;

        var href = match.Groups["href"].Value;
        return Uri.TryCreate(pageUrl, href, out var uri) && uri.Scheme is "http" or "https"
            ? uri
            : null;
    }

    private static string? ExtractDuvDetailRowValue(string html, string label)
    {
        if (string.IsNullOrWhiteSpace(html) || string.IsNullOrWhiteSpace(label))
            return null;

        var pattern = @$"<tr[^>]*>\s*<td[^>]*><b>{Regex.Escape(label)}:\s*</b></td>\s*<td[^>]*>(?<value>.*?)</td>";
        var match = Regex.Match(html, pattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        if (!match.Success)
            return null;

        var value = HtmlDecodeAndStripTags(match.Groups["value"].Value);
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static double? ParseDuvElevationGain(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return null;

        var normalized = text.Replace(" ", "").Replace("\u00A0", "");
        var match = Regex.Match(normalized, @"(?<value>[\d,.]+)\s*(?<unit>ft|m)\b", RegexOptions.IgnoreCase);
        if (!match.Success)
            return null;

        var raw = match.Groups["value"].Value.Replace(",", "", StringComparison.Ordinal);
        if (!double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            return null;

        var unit = match.Groups["unit"].Value.ToLowerInvariant();
        return unit == "ft"
            ? Math.Round(value * 0.3048)
            : value;
    }

    private static string HtmlDecodeAndStripTags(string html)
    {
        return HtmlText.DecodeAndStripTags(html);
    }

    public static (double lat, double lng)? ExtractStartPositionCoordinates(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var jsMatch = DuvStartPosLatLngRegex().Match(html);
        if (jsMatch.Success
            && double.TryParse(jsMatch.Groups["lat"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var lat)
            && double.TryParse(jsMatch.Groups["lng"].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out var lng))
        {
            return (lat, lng);
        }

        var inputMatch = DuvStartPosInputRegex().Match(html);
        if (inputMatch.Success)
        {
            var coords = inputMatch.Groups["coords"].Value.Split(',', StringSplitOptions.TrimEntries);
            if (coords.Length == 2
                && double.TryParse(coords[0], NumberStyles.Float, CultureInfo.InvariantCulture, out lat)
                && double.TryParse(coords[1], NumberStyles.Float, CultureInfo.InvariantCulture, out lng))
            {
                return (lat, lng);
            }
        }

        return null;
    }

    [GeneratedRegex(@"<tr\s+class=['"" ](?:odd|even)['"" ][^>]*>(.*?)</tr>", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex DuvCalendarRowRegex();

    [GeneratedRegex(@"<td[^>]*>(.*?)</td>", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex TableCellRegex();

    [GeneratedRegex(@"<a[^>]+href=['"" ](?<href>[^'"" ]+)['"" ][^>]*>(?<text>.*?)</a>", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex DuvEventLinkRegex();

    [GeneratedRegex(@"[?&]event=(?<id>\d+)", RegexOptions.IgnoreCase)]
    private static partial Regex DuvEventIdRegex();

    [GeneratedRegex(@"(?i)\((?<code>[A-Z]{3})\)\s*$")]
    private static partial Regex DuvCountryCodeRegex();

    [GeneratedRegex(@"<a[^>]+href=['"" ](?<href>[^'"" ]*startpos\.php\?event=\d+)[^'"" ]*['"" ][^>]*>", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex DuvStartPosLinkRegex();

    [GeneratedRegex(@"new\s+google\.maps\.LatLng\(\s*(?<lat>-?\d+(?:\.\d+)?)\s*,\s*(?<lng>-?\d+(?:\.\d+)?)\s*\)", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex DuvStartPosLatLngRegex();

    [GeneratedRegex(@"id=['"" ]startpos['"" ][^>]*value=['"" ](?<coords>[-\d\.]+\s*,\s*[-\d\.]+)['"" ]", RegexOptions.Singleline | RegexOptions.IgnoreCase)]
    private static partial Regex DuvStartPosInputRegex();
}
