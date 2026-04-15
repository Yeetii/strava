using System.Text.Json;
using System.Text.RegularExpressions;

namespace Backend;

public static partial class RaceScrapeDiscovery
{
    public static bool IsUtmbSearchApi(Uri sourceUrl)
    {
        return string.Equals(sourceUrl.Host, "api.utmb.world", StringComparison.OrdinalIgnoreCase)
            && sourceUrl.AbsolutePath.Contains("/search/races", StringComparison.OrdinalIgnoreCase);
    }

    // Parses the response from https://api.utmb.world/search/races?lang=en&limit=400
    // Each race object has:
    //   slug        – the race page URL, e.g. "https://julianalps.utmb.world/races/120K"
    //   name        – race name
    //   details.statsUp – array of { name, value, postfix } where name in {"distance","elevationGain"}
    public static IReadOnlyCollection<RacePageCandidate> ParseUtmbRacePages(string jsonPayload)
    {
        if (string.IsNullOrWhiteSpace(jsonPayload))
            return [];

        using var document = JsonDocument.Parse(jsonPayload);
        var root = document.RootElement;

        if (!root.TryGetProperty("races", out var racesElement) || racesElement.ValueKind != JsonValueKind.Array)
            return [];

        var pageCandidates = new List<RacePageCandidate>();

        foreach (var race in racesElement.EnumerateArray())
        {
            if (race.ValueKind != JsonValueKind.Object)
                continue;

            // slug holds the race page URL directly
            if (!TryGetPropertyIgnoreCase(race, "slug", out var slugElement) || slugElement.ValueKind != JsonValueKind.String)
                continue;

            var slugValue = slugElement.GetString();
            if (!Uri.TryCreate(slugValue, UriKind.Absolute, out var pageUri) || pageUri.Scheme is not ("http" or "https"))
                continue;

            var name = FindStringValue(race, ["name", "title"]);

            double? distance = null;
            double? elevationGain = null;

            if (TryGetPropertyIgnoreCase(race, "details", out var details) && details.ValueKind == JsonValueKind.Object
                && TryGetPropertyIgnoreCase(details, "statsUp", out var statsUp) && statsUp.ValueKind == JsonValueKind.Array)
            {
                foreach (var stat in statsUp.EnumerateArray())
                {
                    if (stat.ValueKind != JsonValueKind.Object)
                        continue;

                    if (!TryGetPropertyIgnoreCase(stat, "name", out var statName) || statName.ValueKind != JsonValueKind.String)
                        continue;

                    var key = statName.GetString();
                    if (!TryGetPropertyIgnoreCase(stat, "value", out var statValue))
                        continue;

                    double? parsed = statValue.ValueKind == JsonValueKind.Number && statValue.TryGetDouble(out var d) ? d : null;

                    if (string.Equals(key, "distance", StringComparison.OrdinalIgnoreCase))
                        distance = parsed;
                    else if (string.Equals(key, "elevationGain", StringComparison.OrdinalIgnoreCase))
                        elevationGain = parsed;
                }
            }

            pageCandidates.Add(new RacePageCandidate(pageUri, name, distance, elevationGain));
        }

        return pageCandidates
            .GroupBy(c => c.PageUrl.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList();
    }

    public static IReadOnlyCollection<Uri> ExtractGpxUrlsFromHtml(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var matches = new List<string>();
        matches.AddRange(HrefRegex().Matches(html).Select(match => match.Groups["href"].Value));
        matches.AddRange(AbsoluteGpxRegex().Matches(html).Select(match => match.Groups["url"].Value));
        matches.AddRange(RelativeGpxRegex().Matches(html).Select(match => match.Groups["url"].Value));

        return matches
            .Select(UnescapeJsonSlash)
            .Select(url => Uri.TryCreate(pageUrl, url, out var parsed) ? parsed : null)
            .Where(uri => uri is { Scheme: "http" or "https" })
            .Cast<Uri>()
            .Where(uri => uri.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
            .Distinct()
            .ToList();
    }

    private static string? FindStringValue(JsonElement element, IEnumerable<string> keys)
    {
        foreach (var key in keys)
        {
            if (TryGetPropertyIgnoreCase(element, key, out var value) && value.ValueKind == JsonValueKind.String)
                return value.GetString();
        }

        return null;
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

    private static string UnescapeJsonSlash(string value)
    {
        return value.Replace("\\/", "/", StringComparison.Ordinal);
    }

    [GeneratedRegex("href\\s*=\\s*[\"'](?<href>[^\"']+)[\"']", RegexOptions.IgnoreCase)]
    private static partial Regex HrefRegex();

    // Matches both normal URLs (https://...) and JSON-escaped URLs (https:\/\/...).
    [GeneratedRegex("(?<url>https?:\\\\?/\\\\?/[^\"'\\s<>]+?\\.gpx(?:\\?[^\"'\\s<>]*)?)", RegexOptions.IgnoreCase)]
    private static partial Regex AbsoluteGpxRegex();

    [GeneratedRegex("(?<url>/[^\"'\\s<>]+?\\.gpx(?:\\?[^\"'\\s<>]*)?)", RegexOptions.IgnoreCase)]
    private static partial Regex RelativeGpxRegex();
}

public record RacePageCandidate(Uri PageUrl, string? Name, double? Distance, double? ElevationGain);
