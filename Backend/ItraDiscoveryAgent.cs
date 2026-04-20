using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using static Backend.RaceScrapeDiscovery;

namespace Backend;

public static partial class ItraDiscoveryAgent
{
    public static readonly Uri CalendarUrl = new("https://itra.run/Races/RaceCalendar");

    private static readonly Regex RequestVerificationTokenRegex = new(
        "name=\"__RequestVerificationToken\"[^>]*value=\"(?<token>[^\"]+)\"",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex EventUrlRegex = new(
        @"<div\s+class='event_name'>.*?<a\s+href='(?<href>[^']+)'",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex EventNameRegex = new(
        @"<h4>(?<name>.*?)</h4>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex DateRegex = new(
        @"<div\s+class='date'>(?<date>.*?)</div>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex LocationRegex = new(
        @"<div\s+class='location'>(?<location>[^<]+)",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex DistanceBoxRegex = new(
        @"<div\s+class='count'>(?<distance>[^<]+)</div>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex H1Regex = new(
        @"<h1\b[^>]*>(?<text>.*?)</h1>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex H3Regex = new(
        @"<h3\b[^>]*>(?<text>.*?)</h3>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex DescriptionAfterH1Regex = new(
        @"<h1\b[^>]*>.*?</h1>\s*<div\b[^>]*\bcol-lg-9\b[^>]*>(?<text>.*?)</div>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex DescriptionAfterH3Regex = new(
        @"<h3\b[^>]*>.*?</h3>\s*<div\b[^>]*\bcol-lg-9\b[^>]*>(?<text>.*?)</div>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex DescriptionAfterAboutRaceRegex = new(
        @"<h3\b[^>]*>\s*About the Race\s*</h3>.*?<div\b[^>]*\bcol-12\b[^>]*>(?<text>.*?)</div>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex DistanceLabelRegex = new(
        @"Distance:\s*<span\b[^>]*>(?<distance>[^<]+)</span>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex RegisterLinkRegex = new(
        @"<a\b[^>]*href=[""'](?<href>[^""']+)[""'][^>]*>\s*Register to this race\s*</a>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex ItraRaceButtonGroupRegex = new(
        @"<div\b[^>]*class=[""'][^""']*\bbtn-group\b[^""']*[""'][^>]*>(?<content>.*?)</div>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex ItraRaceButtonLinkRegex = new(
        @"<a\b[^>]*href=[""'](?<href>[^""']+)[""'][^>]*>\s*(?<text>[^<]+)\s*</a>",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.Singleline);

    private static readonly Regex NationalLeagueRegex = new(
        @"National League\s*(?:[:\-]?\s*(?<value>yes|no|true|false|1|0))?",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public static string? ExtractRequestVerificationToken(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var match = RequestVerificationTokenRegex.Match(html);
        return match.Success ? WebUtility.HtmlDecode(match.Groups["token"].Value) : null;
    }

    public static async Task<IReadOnlyCollection<ScrapeJob>> EnrichEventPageDetailsAsync(
        IReadOnlyCollection<ScrapeJob> jobs,
        HttpClient httpClient,
        CancellationToken cancellationToken)
    {
        if (jobs.Count == 0)
            return jobs;

        var enriched = new List<ScrapeJob>();
        var processedPageUrls = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<ScrapeJob>(jobs);

        while (queue.Count > 0)
        {
            var job = queue.Dequeue();
            var pageUrl = job.ItraEventPageUrl ?? job.WebsiteUrl;
            if (pageUrl is null)
            {
                enriched.Add(job);
                continue;
            }

            if (!processedPageUrls.Add(pageUrl.AbsoluteUri))
                continue;

            try
            {
                using var response = await httpClient.GetAsync(pageUrl, cancellationToken);
                if (!response.IsSuccessStatusCode)
                {
                    enriched.Add(job);
                    continue;
                }

                var html = await response.Content.ReadAsStringAsync(cancellationToken);
                var enrichedJob = EnrichJobFromEventPageHtml(job, html, pageUrl);
                enriched.Add(enrichedJob);

                foreach (var buttonUrl in ExtractItraRaceButtonLinks(html, pageUrl))
                {
                    if (!processedPageUrls.Contains(buttonUrl.AbsoluteUri))
                        queue.Enqueue(new ScrapeJob(WebsiteUrl: buttonUrl, RaceType: "trail"));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                enriched.Add(job);
            }
        }

        return enriched;
    }

    private static Uri? ExtractRegisterLink(string html, Uri baseUrl)
    {
        var match = RegisterLinkRegex.Match(html);
        if (!match.Success)
            return null;

        var href = match.Groups["href"].Value;
        return Uri.TryCreate(baseUrl, href, out var uri) && (uri.Scheme == "http" || uri.Scheme == "https")
            ? uri
            : null;
    }

    public static ScrapeJob EnrichJobFromEventPageHtml(ScrapeJob job, string html, Uri pageUrl)
    {
        var registerUrl = ExtractRegisterLink(html, pageUrl);
        var name = HtmlDecodeAndStripTags(ExtractItraEventName(html));
        var description = HtmlDecodeAndStripTags(ExtractItraEventDescription(html));
        var elevationGain = RaceHtmlScraper.ExtractElevationGain(html);
        var itraPoints = RaceHtmlScraper.ExtractItraPoints(html) ?? RaceHtmlScraper.ExtractTraceDeTrailItraPoints(html);
        var nationalLeague = ExtractItraNationalLeague(html);
        var pageDistance = ExtractItraEventDistance(html);
        var raceType = string.IsNullOrWhiteSpace(job.RaceType) ? "trail" : job.RaceType;

        return job with
        {
            WebsiteUrl = registerUrl ?? job.WebsiteUrl,
            ItraEventPageUrl = pageUrl,
            ItraNationalLeague = nationalLeague ?? job.ItraNationalLeague,
            Name = string.IsNullOrWhiteSpace(name) ? job.Name : name.Trim(),
            Description = string.IsNullOrWhiteSpace(description) ? job.Description : description.Trim(),
            ElevationGain = elevationGain ?? job.ElevationGain,
            ItraPoints = itraPoints ?? job.ItraPoints,
            Distance = pageDistance ?? job.Distance,
            RaceType = raceType
        };
    }

    private static IReadOnlyList<Uri> ExtractItraRaceButtonLinks(string html, Uri baseUrl)
    {
        return ExtractItraRaceButtonLinksWithText(html, baseUrl)
            .Select(link => link.Url)
            .DistinctBy(u => u.AbsoluteUri)
            .ToList();
    }

    private static IReadOnlyList<(Uri Url, string Text)> ExtractItraRaceButtonLinksWithText(string html, Uri baseUrl)
    {
        var links = new List<(Uri Url, string Text)>();
        foreach (Match groupMatch in ItraRaceButtonGroupRegex.Matches(html))
        {
            var content = groupMatch.Groups["content"].Value;
            foreach (Match linkMatch in ItraRaceButtonLinkRegex.Matches(content))
            {
                var href = linkMatch.Groups["href"].Value;
                var text = linkMatch.Groups["text"].Value;
                if (!Uri.TryCreate(baseUrl, href, out var uri) || (uri.Scheme != "http" && uri.Scheme != "https"))
                    continue;

                if (!uri.AbsolutePath.Contains("/Races/RaceDetails/", StringComparison.OrdinalIgnoreCase))
                    continue;

                links.Add((uri, text));
            }
        }
        return links.DistinctBy(link => link.Url.AbsoluteUri).ToList();
    }

    private static string ExtractItraEventName(string html)
    {
        var name = ExtractText(H1Regex, html);
        if (!string.IsNullOrWhiteSpace(name))
            return name;

        return ExtractText(H3Regex, html);
    }

    private static string ExtractItraEventDescription(string html)
    {
        var description = ExtractText(DescriptionAfterH1Regex, html);
        if (!string.IsNullOrWhiteSpace(description))
            return description;

        description = ExtractText(DescriptionAfterH3Regex, html);
        if (!string.IsNullOrWhiteSpace(description))
            return description;

        description = ExtractText(DescriptionAfterAboutRaceRegex, html);
        if (!string.IsNullOrWhiteSpace(description))
            return description;

        return string.Empty;
    }

    private static string? ExtractItraEventDistance(string html)
    {
        var raw = ExtractText(DistanceLabelRegex, html);
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        return ParseDistanceVerbose(HtmlDecodeAndStripTags(raw));
    }

    private static bool? ExtractItraNationalLeague(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var match = NationalLeagueRegex.Match(html);
        if (!match.Success)
            return null;

        var text = match.Groups["value"].Value;
        if (string.IsNullOrWhiteSpace(text))
            return true;

        return text.Trim().ToLowerInvariant() switch
        {
            "yes" => true,
            "true" => true,
            "1" => true,
            "no" => false,
            "false" => false,
            "0" => false,
            _ => null
        };
    }

    public static IReadOnlyCollection<ScrapeJob> ParseCalendarPage(string html, Uri baseUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var fragments = ParseRaceSearchJsonFragments(html);
        if (fragments.Count == 0)
            return [];

        var jobs = new List<ScrapeJob>(fragments.Count);
        foreach (var fragment in fragments)
        {
            var eventUrl = ExtractEventUrl(fragment, baseUrl);
            if (eventUrl is null)
                continue;

            var name = ExtractText(EventNameRegex, fragment);
            var rawDate = HtmlDecodeAndStripTags(ExtractText(DateRegex, fragment));
            var parsedDate = NormalizeDateToYyyyMmDd(rawDate) ?? rawDate;
            var rawLocation = HtmlDecodeAndStripTags(ExtractText(LocationRegex, fragment));

            string? location = null;
            string? country = null;
            if (!string.IsNullOrWhiteSpace(rawLocation))
            {
                var parts = rawLocation.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length > 1)
                {
                    var candidateCountry = NormalizeCountryToIso2(parts[^1]) ?? parts[^1];
                    country = string.IsNullOrWhiteSpace(candidateCountry) ? null : candidateCountry;
                    location = string.Join(", ", parts[..^1]).Trim();
                }
                else
                {
                    location = rawLocation.Trim();
                }
            }

            var distances = DistanceBoxRegex.Matches(fragment)
                .Select(m => ParseDistanceVerbose(HtmlDecodeAndStripTags(m.Groups["distance"].Value.Trim())))
                .Where(d => !string.IsNullOrWhiteSpace(d))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            var distance = distances.Count > 0 ? string.Join(", ", distances) : null;

            var buttonLinks = ExtractItraRaceButtonLinksWithText(fragment, baseUrl);
            if (buttonLinks.Count > 0)
            {
                foreach (var buttonLink in buttonLinks)
                {
                    jobs.Add(new ScrapeJob(
                        WebsiteUrl: buttonLink.Url,
                        Name: string.IsNullOrWhiteSpace(buttonLink.Text) ? null : buttonLink.Text.Trim(),
                        ExternalIds: ExtractExternalIds(buttonLink.Url),
                        RaceType: "trail",
                        Date: parsedDate,
                        Country: country,
                        Location: string.IsNullOrWhiteSpace(location) ? null : location));
                }
                continue;
            }

            var externalIds = ExtractExternalIds(eventUrl);

            jobs.Add(new ScrapeJob(
                WebsiteUrl: eventUrl,
                Name: string.IsNullOrWhiteSpace(name) ? null : name.Trim(),
                ExternalIds: externalIds,
                RaceType: "trail",
                Distance: distance,
                Date: parsedDate,
                Country: country,
                Location: string.IsNullOrWhiteSpace(location) ? null : location));
        }

        return jobs;
    }

    private static IReadOnlyList<string> ParseRaceSearchJsonFragments(string html)
    {
        var marker = "var raceSearchJsonSidePopupNew = [";
        var start = html.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (start < 0)
            return [];

        start += marker.Length;
        var end = html.IndexOf("];", start, StringComparison.OrdinalIgnoreCase);
        if (end < 0)
            return [];

        var arrayBody = html[start..end];
        var fragments = new List<string>();
        int i = 0;

        while (i < arrayBody.Length)
        {
            while (i < arrayBody.Length && (char.IsWhiteSpace(arrayBody[i]) || arrayBody[i] == ',' || arrayBody[i] == '[' || arrayBody[i] == ']'))
                i++;

            if (i >= arrayBody.Length)
                break;
            if (arrayBody[i] != '"')
                break;

            i++;
            var sb = new StringBuilder();
            while (i < arrayBody.Length)
            {
                var c = arrayBody[i++];
                if (c == '\\' && i < arrayBody.Length)
                {
                    sb.Append(arrayBody[i]);
                    i++;
                    continue;
                }

                if (c == '"')
                    break;

                sb.Append(c);
            }

            fragments.Add(sb.ToString());
        }

        return fragments;
    }

    private static Uri? ExtractEventUrl(string html, Uri baseUrl)
    {
        var match = EventUrlRegex.Match(html);
        if (!match.Success)
            return null;

        var href = match.Groups["href"].Value;
        return Uri.TryCreate(baseUrl, href, out var uri) && (uri.Scheme == "http" || uri.Scheme == "https")
            ? uri
            : null;
    }

    private static string ExtractText(Regex regex, string html)
    {
        var match = regex.Match(html);
        return match.Success ? match.Groups[1].Value : string.Empty;
    }

    private static string HtmlDecodeAndStripTags(string html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return string.Empty;

        var decoded = WebUtility.HtmlDecode(html);
        return Regex.Replace(decoded, "<[^>]+>", " ", RegexOptions.Singleline).Trim();
    }

    private static Dictionary<string, string>? ExtractExternalIds(Uri url)
    {
        var segments = url.Segments.Select(s => s.Trim('/')).Where(s => !string.IsNullOrWhiteSpace(s)).ToArray();
        if (segments.Length == 0)
            return null;

        var last = segments[^1];
        return int.TryParse(last, out _) ? new Dictionary<string, string>(StringComparer.Ordinal) { ["itraEventId"] = last } : null;
    }
}
