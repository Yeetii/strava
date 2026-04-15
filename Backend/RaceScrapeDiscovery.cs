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

    // Parses the response from https://www.loppkartan.se/markers-se.json
    // Response shape: { generatedAt, country, markers: [{ id, name, latitude, longitude, ... }] }
    public static IReadOnlyCollection<LoppkartanScrapeTarget> ParseLoppkartanMarkers(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
            return [];

        if (!TryGetPropertyIgnoreCase(root, "markers", out var markersEl) || markersEl.ValueKind != JsonValueKind.Array)
            return [];

        var targets = new List<LoppkartanScrapeTarget>();
        foreach (var marker in markersEl.EnumerateArray())
        {
            if (marker.ValueKind != JsonValueKind.Object)
                continue;

            var markerId = FindStringValue(marker, ["id"]);
            if (string.IsNullOrWhiteSpace(markerId))
                continue;

            if (!TryGetDoubleValue(marker, "latitude", out var latitude))
                continue;
            if (!TryGetDoubleValue(marker, "longitude", out var longitude))
                continue;

            targets.Add(new LoppkartanScrapeTarget(
                MarkerId: markerId,
                Name: FindStringValue(marker, ["name"]),
                Latitude: latitude,
                Longitude: longitude,
                Website: FindStringValue(marker, ["website"]),
                Location: FindStringValue(marker, ["location"]),
                County: FindStringValue(marker, ["county"]),
                RaceDate: FindStringValue(marker, ["race_date"]),
                RaceType: FindStringValue(marker, ["race_type"]),
                TypeLocal: FindStringValue(marker, ["type_local"]),
                DomainName: FindStringValue(marker, ["domain_name"]),
                OriginCountry: FindStringValue(marker, ["origin_country"]),
                DistanceVerbose: FindStringValue(marker, ["distance_verbose"])));
        }

        return targets
            .GroupBy(t => t.MarkerId, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
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

    private static bool TryGetDoubleValue(JsonElement element, string key, out double value)
    {
        value = default;
        if (!TryGetPropertyIgnoreCase(element, key, out var property))
            return false;

        if (property.ValueKind == JsonValueKind.Number && property.TryGetDouble(out value))
            return true;

        if (property.ValueKind == JsonValueKind.String
            && double.TryParse(property.GetString(), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out value))
            return true;

        return false;
    }

    // Parses the response from POST https://tracedetrail.fr/event/getEventsCalendar/all/all/all
    // Each event has traceIDs (underscore-separated ints), distances (underscore-separated km values), nom
    public static IReadOnlyCollection<TraceDeTrailScrapeTarget> ParseTraceDeTrailCalendarEvents(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        // Response is {"success":1,"data":[...]}
        if (root.ValueKind == JsonValueKind.Object
            && TryGetPropertyIgnoreCase(root, "data", out var dataEl)
            && dataEl.ValueKind == JsonValueKind.Array)
        {
            root = dataEl;
        }
        else if (root.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var targets = new List<TraceDeTrailScrapeTarget>();

        foreach (var evt in root.EnumerateArray())
        {
            if (evt.ValueKind != JsonValueKind.Object)
                continue;

            if (!TryGetPropertyIgnoreCase(evt, "traceIDs", out var traceIDsEl) || traceIDsEl.ValueKind != JsonValueKind.String)
                continue;

            var traceIds = (traceIDsEl.GetString() ?? string.Empty)
                .Split('_', StringSplitOptions.RemoveEmptyEntries);

            if (traceIds.Length == 0)
                continue;

            var name = FindStringValue(evt, ["nom", "name"]);

            string[]? distanceParts = null;
            if (TryGetPropertyIgnoreCase(evt, "distances", out var distancesEl) && distancesEl.ValueKind == JsonValueKind.String)
                distanceParts = distancesEl.GetString()?.Split('_', StringSplitOptions.RemoveEmptyEntries);

            for (int i = 0; i < traceIds.Length; i++)
            {
                if (!int.TryParse(traceIds[i], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var traceId))
                    continue;

                double? distance = null;
                if (distanceParts != null && i < distanceParts.Length &&
                    double.TryParse(distanceParts[i], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var d))
                    distance = d;

                targets.Add(new TraceDeTrailScrapeTarget(traceId, name, distance));
            }
        }

        return targets;
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
                    && double.TryParse(distEl.GetString(), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var d))
                    distance = d;
                if (TryGetPropertyIgnoreCase(traceEl, "dev_pos", out var gainEl) && gainEl.ValueKind == JsonValueKind.String
                    && double.TryParse(gainEl.GetString(), System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var g))
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

public record RaceScrapeTarget(
    Uri GpxUrl,
    Uri SourceUrl,
    Uri CoursePageUrl,
    string? Name,
    double? Distance,
    double? ElevationGain);

public record TraceDeTrailScrapeTarget(int TraceId, string? Name, double? Distance);

public record LoppkartanScrapeTarget(
    string MarkerId,
    string? Name,
    double Latitude,
    double Longitude,
    string? Website,
    string? Location,
    string? County,
    string? RaceDate,
    string? RaceType,
    string? TypeLocal,
    string? DomainName,
    string? OriginCountry,
    string? DistanceVerbose);

public record TraceDeTrailTraceData(
    IReadOnlyList<(double Lng, double Lat)> Points,
    double? TotalDistanceKm,
    double? ElevationGain);
