using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Backend;

public static partial class RaceScrapeDiscovery
{
    private static readonly Uri UtmbBaseUri = new("https://utmb.world");

    public static bool IsUtmbSearchApi(Uri sourceUrl)
    {
        return string.Equals(sourceUrl.Host, "api.utmb.world", StringComparison.OrdinalIgnoreCase)
            && sourceUrl.AbsolutePath.Contains("/search/races", StringComparison.OrdinalIgnoreCase);
    }

    public static IReadOnlyCollection<RacePageCandidate> ParseUtmbRacePages(string jsonPayload)
    {
        if (string.IsNullOrWhiteSpace(jsonPayload))
            return [];

        using var document = JsonDocument.Parse(jsonPayload);
        var raceObjects = EnumerateRaceObjects(document.RootElement).ToList();
        var pageCandidates = new List<RacePageCandidate>();

        foreach (var raceObject in raceObjects)
        {
            var pageUri = FindRacePageUri(raceObject);
            if (pageUri is null)
                continue;

            pageCandidates.Add(new RacePageCandidate(
                pageUri,
                FindStringValue(raceObject, ["name", "title", "raceName"]),
                FindNumericValue(raceObject, ["distance", "distanceKm", "distance_km"]),
                FindNumericValue(raceObject, ["elevationGain", "elevation_gain", "elevation", "dPlus", "ascent"])));
        }

        return pageCandidates
            .GroupBy(candidate => candidate.PageUrl.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
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

    private static IEnumerable<JsonElement> EnumerateRaceObjects(JsonElement root)
    {
        if (root.ValueKind == JsonValueKind.Array)
        {
            return root.EnumerateArray().Where(element => element.ValueKind == JsonValueKind.Object);
        }

        if (root.ValueKind != JsonValueKind.Object)
            return [];

        foreach (var arrayName in new[] { "results", "races", "items", "data" })
        {
            if (root.TryGetProperty(arrayName, out var arrayElement) && arrayElement.ValueKind == JsonValueKind.Array)
            {
                return arrayElement.EnumerateArray().Where(element => element.ValueKind == JsonValueKind.Object);
            }
        }

        return root.EnumerateObject()
            .Where(property => property.Value.ValueKind == JsonValueKind.Object)
            .Select(property => property.Value);
    }

    private static Uri? FindRacePageUri(JsonElement raceObject)
    {
        foreach (var key in new[] { "url", "websiteUrl", "website", "link", "raceUrl", "webUrl" })
        {
            var value = FindStringValue(raceObject, [key]);
            if (TryBuildRacePageUri(value, out var uri))
                return uri;
        }

        foreach (var property in raceObject.EnumerateObject())
        {
            if (property.Value.ValueKind != JsonValueKind.String)
                continue;

            var value = property.Value.GetString();
            if (value is null)
                continue;

            if (!property.Name.Contains("url", StringComparison.OrdinalIgnoreCase)
                && !property.Name.Contains("link", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (TryBuildRacePageUri(value, out var uri))
                return uri;
        }

        return null;
    }

    private static bool TryBuildRacePageUri(string? value, out Uri? uri)
    {
        uri = null;
        if (string.IsNullOrWhiteSpace(value))
            return false;
        if (value.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
            return false;

        if (Uri.TryCreate(value, UriKind.Absolute, out var absolute) && absolute.Scheme is "http" or "https")
        {
            uri = absolute;
            return true;
        }

        if (Uri.TryCreate(UtmbBaseUri, value, out var relative) && relative.Scheme is "http" or "https")
        {
            uri = relative;
            return true;
        }

        return false;
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

    private static double? FindNumericValue(JsonElement element, IEnumerable<string> keys)
    {
        foreach (var key in keys)
        {
            if (!TryGetPropertyIgnoreCase(element, key, out var value))
                continue;

            if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var number))
                return number;

            if (value.ValueKind == JsonValueKind.String
                && double.TryParse(value.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out number))
            {
                return number;
            }
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

    [GeneratedRegex("(?<url>https?:\\\\?/\\\\?/[^\"'\\s<>]+?\\.gpx(?:\\?[^\"'\\s<>]*)?)", RegexOptions.IgnoreCase)]
    private static partial Regex AbsoluteGpxRegex();

    [GeneratedRegex("(?<url>/[^\"'\\s<>]+?\\.gpx(?:\\?[^\"'\\s<>]*)?)", RegexOptions.IgnoreCase)]
    private static partial Regex RelativeGpxRegex();
}

public record RacePageCandidate(Uri PageUrl, string? Name, double? Distance, double? ElevationGain);
