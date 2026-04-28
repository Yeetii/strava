using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;

namespace Backend;

public static partial class SkyrunningDiscoveryAgent
{
    public static readonly Uri CalendarUrl = new("https://www.skyrunning.com/calendar/");

    public static IReadOnlyCollection<ScrapeJob> ParseCalendarPage(string html, Uri baseUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var table = ExtractHtmlElement(html, "table", "race_list");
        if (string.IsNullOrWhiteSpace(table))
            return [];

        var jobs = new List<ScrapeJob>();
        foreach (Match rowMatch in Regex.Matches(table, "<tr[^>]*>(.*?)</tr>", RegexOptions.IgnoreCase | RegexOptions.Singleline))
        {
            var row = rowMatch.Groups[1].Value;
            if (row.Contains("class=\"ttitle\"", StringComparison.OrdinalIgnoreCase))
                continue;

            var cells = Regex.Matches(row, "<td[^>]*>(.*?)</td>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (cells.Count < 6)
                continue;

            var logoHtml = cells[0].Groups[1].Value;
            var nameHtml = cells[2].Groups[1].Value;
            var dateHtml = cells[3].Groups[1].Value;
            var countryHtml = cells[4].Groups[1].Value;
            var disciplineHtml = cells[5].Groups[1].Value;

            var eventUrl = ResolveUri(ExtractAttributeValue(nameHtml, "href"), baseUrl);
            if (eventUrl is null)
                continue;

            var name = NormalizeWhitespace(StripHtml(nameHtml));
            if (string.IsNullOrWhiteSpace(name))
                continue;

            var date = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(StripHtml(dateHtml));
            var country = NormalizeWhitespace(StripHtml(countryHtml));
            var raceType = NormalizeWhitespace(StripHtml(disciplineHtml));
            var logoUrl = ExtractAttributeValue(logoHtml, "src");

            jobs.Add(new ScrapeJob(
                WebsiteUrl: eventUrl,
                Name: name,
                Date: date,
                Country: country,
                RaceType: raceType,
                LogoUrl: logoUrl));
        }

        return jobs;
    }

    public static async Task<ScrapeJob> EnrichJobAsync(HttpClient httpClient, ScrapeJob job, CancellationToken cancellationToken)
    {
        if (job.WebsiteUrl is null)
            return job;

        using var response = await httpClient.GetAsync(job.WebsiteUrl, cancellationToken);
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
            throw new HttpRequestException("Skyrunning event page returned 429", null, response.StatusCode);

        response.EnsureSuccessStatusCode();

        var html = await response.Content.ReadAsStringAsync(cancellationToken);
        return EnrichJobFromEventPageHtml(job, html, job.WebsiteUrl);
    }

    public static ScrapeJob EnrichJobFromEventPageHtml(ScrapeJob job, string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return job;

        var values = ExtractElementTexts(html, "span", "elementor-icon-list-text")
            .Where(v => !string.IsNullOrWhiteSpace(v))
            .ToList();

        string? parsedDate = null;
        string? distance = null;
        double? elevationGain = null;
        string? country = null;
        string? organizer = null;
        string? raceType = null;
        var leftover = new List<string>();

        foreach (var raw in values)
        {
            var value = raw.Trim();
            if (string.IsNullOrWhiteSpace(value))
                continue;

            if (RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(value) is string normalizedDate)
            {
                parsedDate = normalizedDate;
                continue;
            }

            if (value.StartsWith("Distance:", StringComparison.OrdinalIgnoreCase))
            {
                distance = NormalizeWhitespace(value[("Distance:".Length)..]);
                continue;
            }

            if (value.StartsWith("Vertical climb:", StringComparison.OrdinalIgnoreCase))
            {
                elevationGain = ParseElevationGain(value[("Vertical climb:".Length)..]);
                continue;
            }

            if (value.StartsWith("Highest point:", StringComparison.OrdinalIgnoreCase)
                || value.StartsWith("Technical level:", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (IsDisciplineToken(value))
            {
                raceType = value;
                continue;
            }

            if (RaceScrapeDiscovery.NormalizeCountryToIso2(value) is not null)
            {
                country = value;
                continue;
            }

            leftover.Add(value);
        }

        var siteUrl = ExtractEventSiteUrl(html, pageUrl) ?? job.WebsiteUrl;
        if (country is not null)
            country = RaceScrapeDiscovery.NormalizeCountryToIso2(country) ?? country;

        if (leftover.Count > 0)
            organizer = leftover.Last();

        return job with
        {
            WebsiteUrl = siteUrl,
            Date = parsedDate ?? job.Date,
            Distance = distance ?? job.Distance,
            ElevationGain = elevationGain ?? job.ElevationGain,
            Country = country ?? job.Country,
            Organizer = organizer ?? job.Organizer,
            RaceType = job.RaceType ?? raceType
        };
    }

    private static Uri? ExtractEventSiteUrl(string html, Uri pageUrl)
    {
        foreach (Match match in Regex.Matches(html, "<a[^>]*href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>", RegexOptions.IgnoreCase | RegexOptions.Singleline))
        {
            var anchorContent = match.Groups[2].Value;
            if (!anchorContent.Contains("elementor-icon-list-text", StringComparison.OrdinalIgnoreCase))
                continue;

            var href = WebUtility.HtmlDecode(match.Groups[1].Value.Trim());
            if (string.IsNullOrWhiteSpace(href))
                continue;

            if (!Uri.TryCreate(href, UriKind.Absolute, out var uri)
                || (uri.Scheme != "http" && uri.Scheme != "https"))
                continue;

            if (uri.Host.EndsWith("skyrunning.com", StringComparison.OrdinalIgnoreCase))
                continue;

            return uri;
        }

        return null;
    }

    private static bool IsDisciplineToken(string value)
    {
        return string.Equals(value, "Sky", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SkySnow", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "Vertical", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "SkyUltra", StringComparison.OrdinalIgnoreCase);
    }

    private static double? ParseElevationGain(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var match = Regex.Match(value, "([0-9][0-9.,]*)");
        if (!match.Success)
            return null;

        var normalized = match.Groups[1].Value.Replace(",", string.Empty);
        return double.TryParse(normalized, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var result)
            ? result
            : null;
    }

    private static IReadOnlyCollection<string> ExtractElementTexts(string html, string elementName, string classContains)
    {
        var results = new List<string>();
        var pattern = $"<\\s*{elementName}[^>]*class\\s*=\\s*['\"][^'\"]*{Regex.Escape(classContains)}[^'\"]*['\"][^>]*>(.*?)</\\s*{elementName}\\s*>";
        foreach (Match match in Regex.Matches(html, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline))
        {
            var text = StripHtml(match.Groups[1].Value);
            if (!string.IsNullOrWhiteSpace(text))
                results.Add(NormalizeWhitespace(text));
        }
        return results;
    }

    private static string? ExtractHtmlElement(string html, string tagName, string className)
    {
        var pattern = $"<\\s*{tagName}\\b[^>]*\\bclass\\s*=\\s*['\"]([^'\"]*\\b{Regex.Escape(className)}\\b[^'\"]*)['\"][^>]*>";
        var match = Regex.Match(html, pattern, RegexOptions.IgnoreCase);
        if (!match.Success)
            return null;

        var startIndex = match.Index;
        var endTag = $"</{tagName}>";
        var index = html.IndexOf(endTag, startIndex, StringComparison.OrdinalIgnoreCase);
        if (index < 0)
            return null;

        return html.Substring(startIndex, index - startIndex + endTag.Length);
    }

    private static Uri? ResolveUri(string? value, Uri baseUrl)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        if (Uri.TryCreate(value, UriKind.Absolute, out var absoluteUri)
            && (absoluteUri.Scheme == "http" || absoluteUri.Scheme == "https"))
        {
            return absoluteUri;
        }

        return Uri.TryCreate(baseUrl, value, out var relativeUri) ? relativeUri : null;
    }

    private static string NormalizeWhitespace(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        return Regex.Replace(WebUtility.HtmlDecode(text), "\\s+", " ").Trim();
    }

    private static string? ExtractAttributeValue(string html, string attributeName)
    {
        var match = Regex.Match(html, $@"{Regex.Escape(attributeName)}\s*=\s*(?:(['""])(.*?)\1|([^\s>]+))", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success)
            return null;

        var value = match.Groups[2].Success ? match.Groups[2].Value : match.Groups[3].Value;
        return WebUtility.HtmlDecode(value.Trim());
    }

    private static string StripHtml(string? html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return string.Empty;

        return NormalizeWhitespace(Regex.Replace(html, "<[^>]+>", string.Empty));
    }
}
