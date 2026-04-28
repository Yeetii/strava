using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Shared.Services;

namespace Backend;

public static partial class RaceScrapeDiscovery
{
    // Parses the response from:
    // GET https://www.betrail.run/api/events-drizzle?after=...&before=...&scope=full&predicted=1&length=full&offset=...
    // The API response can vary in shape over time; this parser tolerates:
    // - root array: [ { ...event... } ]
    // - root object with array payload under keys like data/items/events/results.
    public static IReadOnlyCollection<ScrapeJob> ParseBeTrailEvents(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryFindEventArray(root, out var eventsEl))
            return [];

        var jobs = new List<ScrapeJob>();
        foreach (var evt in eventsEl.EnumerateArray())
        {
            if (evt.ValueKind != JsonValueKind.Object)
                continue;

            jobs.AddRange(BuildJobsForBeTrailEvent(evt));
        }

        // Deduplicate. Per-race jobs from the same event share a BetrailUrl, so the unique part
        // is the race id (preferred) or distance. Fall back to WebsiteUrl / name+date+distance.
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var unique = new List<ScrapeJob>(jobs.Count);
        foreach (var j in jobs)
        {
            var baseKey = j.BetrailUrl?.AbsoluteUri
                ?? j.WebsiteUrl?.AbsoluteUri
                ?? $"{j.Name}|{j.Date}";
            var raceKey = j.ExternalIds?.GetValueOrDefault("betrail") ?? j.Distance ?? "";
            if (seen.Add($"{baseKey}|{raceKey}"))
                unique.Add(j);
        }
        return unique;
    }

    private static IEnumerable<ScrapeJob> BuildJobsForBeTrailEvent(JsonElement evt)
    {
        // Event-level fields.
        var eventName = FindStringValue(evt, ["title", "event_name", "name"]);
        var eventAlias = FindStringValue(evt, ["alias"]);

        // Prefer trail block data when present (place, country, website, slug).
        var trail = TryGetProperty(evt, "trail");
        var trailAlias2 = trail is { ValueKind: JsonValueKind.Object }
            ? FindStringValue(trail.Value, ["alias2", "alias"]) : null;
        var trailWebsiteRaw = trail is { ValueKind: JsonValueKind.Object }
            ? FindStringValue(trail.Value, ["website"]) : null;
        var country = NormalizeCountryToIso2(
            FindStringValue(evt, ["country"])
            ?? (trail is { ValueKind: JsonValueKind.Object } ? FindStringValue(trail.Value, ["country"]) : null));
        var location = trail is { ValueKind: JsonValueKind.Object }
            ? FindStringValue(trail.Value, ["place", "city", "town"]) : null;

        // Event-level date (unix seconds or string).
        var eventDate = ExtractBeTrailDate(evt, "date") ?? ExtractBeTrailDate(evt, "predicted_next_date");

        var eventBetrailUrl = BuildBeTrailEventUrl(trailAlias2, eventAlias);

        // Only keep the external website when it's not a ranking/result/PDF export.
        var websiteUrl = NormalizeExternalBeTrailWebsite(trailWebsiteRaw);

        var imageUrl = TryExtractBeTrailImage(evt);
        var organizer = trail is { ValueKind: JsonValueKind.Object }
            ? FindStringValue(trail.Value, ["organizer"]) : null;

        // Event-level coordinates (fallback for per-race jobs — races don't carry their own coords).
        double? lat = null, lng = null;
        if (TryGetDoubleValue(evt, "geo_lat", out var evLat)) lat = evLat;
        if (TryGetDoubleValue(evt, "geo_lon", out var evLng)) lng = evLng;
        if ((lat is null || lng is null) && trail is { ValueKind: JsonValueKind.Object })
        {
            if (lat is null && TryGetDoubleValue(trail.Value, "geo_lat", out var tLat)) lat = tLat;
            if (lng is null && TryGetDoubleValue(trail.Value, "geo_lon", out var tLng)) lng = tLng;
        }

        // Per-race entries. When present, we emit one ScrapeJob per race with its own distance / elevation / type.
        var races = TryGetProperty(evt, "races");
        if (races is { ValueKind: JsonValueKind.Array } racesArr && racesArr.GetArrayLength() > 0)
        {
            foreach (var race in racesArr.EnumerateArray())
            {
                if (race.ValueKind != JsonValueKind.Object)
                    continue;

                if (eventBetrailUrl is null)
                    continue;

                var raceId = ExtractScalarAsString(race, "id");
                var externalIds = raceId is not null
                    ? new Dictionary<string, string>(StringComparer.Ordinal) { ["betrail"] = raceId }
                    : null;

                string? distance = null;
                if (TryGetDoubleValue(race, "distance", out var distKm) && distKm > 0)
                    distance = FormatDistanceKm(distKm);

                double? elevation = null;
                if (TryGetDoubleValue(race, "elevation", out var elev) && elev > 0)
                    elevation = elev;

                var raceName = FindStringValue(race, ["title", "race_name"]) ?? eventName;
                var raceDate = ExtractBeTrailDate(race, "date") ?? eventDate;
                var raceType = NormalizeBeTrailRaceType(
                    FindStringValue(race, ["category"]),
                    FindStringValue(race, ["race_type"]));

                yield return new ScrapeJob(
                    WebsiteUrl: websiteUrl,
                    BetrailUrl: eventBetrailUrl,
                    Name: raceName,
                    ExternalIds: externalIds,
                    Distance: distance,
                    ElevationGain: elevation,
                    Date: raceDate,
                    Country: country,
                    Location: location,
                    RaceType: raceType,
                    ImageUrl: imageUrl,
                    Organizer: organizer,
                    Latitude: lat,
                    Longitude: lng);
            }

            yield break;
        }

        // Fallback: no races[] → emit a single event-level job.
        if (eventBetrailUrl is null)
            yield break;

        Dictionary<string, string>? fallbackIds = null;
        var fallbackId = ExtractScalarAsString(evt, "id");
        if (fallbackId is not null)
            fallbackIds = new Dictionary<string, string>(StringComparer.Ordinal) { ["betrail"] = fallbackId };

        yield return new ScrapeJob(
            WebsiteUrl: websiteUrl,
            BetrailUrl: eventBetrailUrl,
            Name: eventName,
            ExternalIds: fallbackIds,
            Date: eventDate,
            Country: country,
            Location: location,
            RaceType: "trail",
            ImageUrl: imageUrl,
            Organizer: organizer,
            Latitude: lat,
            Longitude: lng);
    }

    private static JsonElement? TryGetProperty(JsonElement node, string propertyName)
    {
        if (node.ValueKind != JsonValueKind.Object) return null;
        foreach (var prop in node.EnumerateObject())
        {
            if (string.Equals(prop.Name, propertyName, StringComparison.OrdinalIgnoreCase))
                return prop.Value;
        }
        return null;
    }

    /// <summary>
    /// Reads a scalar property (typically an id) as a string regardless of whether the JSON
    /// encodes it as a number, string, or boolean. Returns <c>null</c> for missing/empty/object/array values.
    /// </summary>
    private static string? ExtractScalarAsString(JsonElement node, string propertyName)
    {
        if (!TryGetPropertyIgnoreCase(node, propertyName, out var el))
            return null;

        return el.ValueKind switch
        {
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            _ => null,
        };
    }

    // Canonical BeTrail event URL: https://www.betrail.run/race/{trail.alias2}/{event.alias}.
    // The site renders the same page for every race inside an event, so per-race URLs don't
    // exist — all races share this one URL. The /en/ language prefix does exist but isn't
    // accepted by every edition, so we use the canonical unprefixed form.
    // Examples: /race/yukon.arctic.ultra/2026, /race/epicurienne-trail/2025.
    private static Uri? BuildBeTrailEventUrl(string? trailAlias, string? eventAlias)
    {
        if (string.IsNullOrWhiteSpace(trailAlias))
            return null;

        var url = string.IsNullOrWhiteSpace(eventAlias)
            ? $"https://www.betrail.run/race/{trailAlias}"
            : $"https://www.betrail.run/race/{trailAlias}/{eventAlias}";

        return Uri.TryCreate(url, UriKind.Absolute, out var uri) ? uri : null;
    }

    private static string? ExtractBeTrailDate(JsonElement node, string propertyName)
    {
        if (!TryGetPropertyIgnoreCase(node, propertyName, out var el))
            return null;

        // BeTrail API delivers dates as unix-seconds integers.
        if (el.ValueKind == JsonValueKind.Number && el.TryGetInt64(out var seconds) && seconds > 0)
        {
            try
            {
                var dt = DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime;
                return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }
            catch
            {
                return null;
            }
        }

        if (el.ValueKind == JsonValueKind.String)
        {
            var raw = el.GetString();
            return NormalizeDateToYyyyMmDd(raw) ?? raw;
        }

        return null;
    }

    private static Uri? NormalizeExternalBeTrailWebsite(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        if (!Uri.TryCreate(RepairDoubledScheme(raw.Trim()), UriKind.Absolute, out var uri)) return null;
        if (uri.Scheme is not ("http" or "https")) return null;

        // Reject result/ranking exports that are not the actual event website.
        var path = uri.AbsolutePath;
        if (path.Contains("/ranking", StringComparison.OrdinalIgnoreCase)
            || uri.Query.Contains("export=pdf", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return uri;
    }

    private static string? TryExtractBeTrailImage(JsonElement evt)
    {
        var trail = TryGetProperty(evt, "trail");
        var photo = trail is { ValueKind: JsonValueKind.Object } ? TryGetProperty(trail.Value, "photo") : null;
        if (photo is { ValueKind: JsonValueKind.Object })
        {
            var key = FindStringValue(photo.Value, ["large_image", "medium_image", "small_image"]);
            if (!string.IsNullOrWhiteSpace(key))
            {
                var absolute = key!.StartsWith("http", StringComparison.OrdinalIgnoreCase)
                    ? key
                    : $"https://www.betrail.run{key}";
                return absolute;
            }
        }

        return FindStringValue(evt, ["image", "imageUrl", "coverImage"]);
    }

    private static string? NormalizeBeTrailRaceType(string? category, string? raceType)
    {
        // The BeTrail category taxonomy (nature, nature_xl, raid, etc.) is more informative than race_type
        // (which is usually just "solo"/"team"). Prefer category; fall back to "trail" if neither helps.
        var token = !string.IsNullOrWhiteSpace(category) ? category : raceType;
        var normalized = NormalizeRaceType(token);
        return !string.IsNullOrWhiteSpace(normalized) ? normalized : "trail";
    }

    private static Uri? TryExtractBeTrailDetailsUrl(JsonElement evt)
    {
        // First, try explicit URL fields (including nested objects).
        if (TryFindStringPropertyRecursive(evt,
                ["url", "href", "link", "raceUrl", "eventUrl", "permalink", "canonicalUrl", "race_url", "event_url", "pathname", "path", "url_en", "path_en", "slug_en"],
                out var urlCandidate))
        {
            var fromUrlField = NormalizeBeTrailRaceUrl(urlCandidate);
            if (fromUrlField is not null)
                return fromUrlField;
        }

        // Fallback to slug fields.
        if (TryFindStringPropertyRecursive(evt,
                ["slug", "eventSlug", "raceSlug", "event_slug", "race_slug", "seoSlug"],
                out var slug))
        {
            return NormalizeBeTrailRaceUrl(slug);
        }

        // Last resort: scan any string value in the object for race-like URL/path content.
        if (TryFindRaceLikeUrlRecursive(evt, out var raceLike))
            return NormalizeBeTrailRaceUrl(raceLike);

        return null;
    }

    private static Uri? NormalizeBeTrailRaceUrl(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        var value = raw.Trim();
        if (Uri.TryCreate(value, UriKind.Absolute, out var absolute))
        {
            if (absolute.Scheme is not ("http" or "https"))
                return null;

            // Keep only canonical race pages from BeTrail.
            if (absolute.Host.Contains("betrail.run", StringComparison.OrdinalIgnoreCase))
            {
                if (absolute.AbsolutePath.StartsWith("/en/race/", StringComparison.OrdinalIgnoreCase))
                    return absolute;

                if (absolute.AbsolutePath.StartsWith("/race/", StringComparison.OrdinalIgnoreCase))
                    return new UriBuilder(absolute) { Path = $"/en{absolute.AbsolutePath}" }.Uri;
            }

            return absolute;
        }

        if (value.StartsWith("/"))
            value = value[1..];

        if (value.StartsWith("calendar/", StringComparison.OrdinalIgnoreCase)
            || value.StartsWith("trailrunning-calendar/", StringComparison.OrdinalIgnoreCase))
            return null;

        if (value.StartsWith("en/race/", StringComparison.OrdinalIgnoreCase))
            return Uri.TryCreate($"https://www.betrail.run/{value}", UriKind.Absolute, out var enRaceUrl)
                ? enRaceUrl
                : null;

        if (!value.StartsWith("race/", StringComparison.OrdinalIgnoreCase))
            value = $"race/{value}";

        return Uri.TryCreate($"https://www.betrail.run/en/{value}", UriKind.Absolute, out var raceUrl)
            ? raceUrl
            : null;
    }

    private static bool IsBetrailUrl(Uri? url) =>
        url is not null
        && url.Scheme is "http" or "https"
        && url.Host.Contains("betrail.run", StringComparison.OrdinalIgnoreCase);

    private static bool TryFindStringPropertyRecursive(
        JsonElement node,
        IReadOnlyCollection<string> keys,
        out string? value)
    {
        value = null;
        var keySet = new HashSet<string>(keys, StringComparer.OrdinalIgnoreCase);

        var queue = new Queue<JsonElement>();
        queue.Enqueue(node);
        int visited = 0;
        const int maxNodes = 400;

        while (queue.Count > 0 && visited < maxNodes)
        {
            visited++;
            var current = queue.Dequeue();

            if (current.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in current.EnumerateObject())
                {
                    if (keySet.Contains(prop.Name) && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        var found = prop.Value.GetString();
                        if (!string.IsNullOrWhiteSpace(found))
                        {
                            value = found;
                            return true;
                        }
                    }

                    if (prop.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                        queue.Enqueue(prop.Value);
                }
            }
            else if (current.ValueKind == JsonValueKind.Array)
            {
                int count = 0;
                foreach (var item in current.EnumerateArray())
                {
                    if (item.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                        queue.Enqueue(item);
                    count++;
                    if (count >= 100) break;
                }
            }
        }

        return false;
    }

    private static bool TryFindRaceLikeUrlRecursive(JsonElement node, out string? value)
    {
        value = null;
        var queue = new Queue<JsonElement>();
        queue.Enqueue(node);
        int visited = 0;
        const int maxNodes = 500;

        while (queue.Count > 0 && visited < maxNodes)
        {
            visited++;
            var current = queue.Dequeue();

            if (current.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in current.EnumerateObject())
                {
                    var propValue = prop.Value;
                    if (propValue.ValueKind == JsonValueKind.String)
                    {
                        var s = propValue.GetString();
                        if (string.IsNullOrWhiteSpace(s))
                            continue;

                        if (s.Contains("betrail.run", StringComparison.OrdinalIgnoreCase)
                            || s.Contains("/race/", StringComparison.OrdinalIgnoreCase)
                            || s.StartsWith("race/", StringComparison.OrdinalIgnoreCase)
                            || s.StartsWith("en/race/", StringComparison.OrdinalIgnoreCase))
                        {
                            value = s;
                            return true;
                        }
                    }
                    else if (propValue.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                    {
                        queue.Enqueue(propValue);
                    }
                }
            }
            else if (current.ValueKind == JsonValueKind.Array)
            {
                int count = 0;
                foreach (var item in current.EnumerateArray())
                {
                    if (item.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                        queue.Enqueue(item);
                    else if (item.ValueKind == JsonValueKind.String)
                    {
                        var s = item.GetString();
                        if (!string.IsNullOrWhiteSpace(s)
                            && (s.Contains("betrail.run", StringComparison.OrdinalIgnoreCase)
                                || s.Contains("/race/", StringComparison.OrdinalIgnoreCase)
                                || s.StartsWith("race/", StringComparison.OrdinalIgnoreCase)
                                || s.StartsWith("en/race/", StringComparison.OrdinalIgnoreCase)))
                        {
                            value = s;
                            return true;
                        }
                    }

                    count++;
                    if (count >= 100) break;
                }
            }
        }

        return false;
    }

    private static bool TryFindEventArray(JsonElement root, out JsonElement eventsEl)
    {
        eventsEl = default;

        if (root.ValueKind == JsonValueKind.Array && LooksLikeObjectArray(root))
        {
            eventsEl = root;
            return true;
        }

        if (root.ValueKind != JsonValueKind.Object)
            return false;

        // BeTrail events-drizzle API wraps the array as { body: { events: [...] } }.
        if (TryGetPropertyIgnoreCase(root, "body", out var bodyEl)
            && bodyEl.ValueKind == JsonValueKind.Object
            && TryGetPropertyIgnoreCase(bodyEl, "events", out eventsEl)
            && eventsEl.ValueKind == JsonValueKind.Array
            && LooksLikeObjectArray(eventsEl))
        {
            return true;
        }

        // Fast-path for known keys.
        if ((TryGetPropertyIgnoreCase(root, "data", out eventsEl)
                || TryGetPropertyIgnoreCase(root, "items", out eventsEl)
                || TryGetPropertyIgnoreCase(root, "events", out eventsEl)
                || TryGetPropertyIgnoreCase(root, "results", out eventsEl)
                || TryGetPropertyIgnoreCase(root, "rows", out eventsEl))
            && eventsEl.ValueKind == JsonValueKind.Array
            && LooksLikeObjectArray(eventsEl))
        {
            return true;
        }

        // Fallback: breadth-first search for the first array that looks like a list of objects.
        var queue = new Queue<JsonElement>();
        queue.Enqueue(root);

        int visited = 0;
        const int maxNodes = 200;

        while (queue.Count > 0 && visited < maxNodes)
        {
            visited++;
            var node = queue.Dequeue();

            if (node.ValueKind == JsonValueKind.Array)
            {
                if (LooksLikeObjectArray(node))
                {
                    eventsEl = node;
                    return true;
                }

                int c = 0;
                foreach (var item in node.EnumerateArray())
                {
                    if (item.ValueKind is JsonValueKind.Array or JsonValueKind.Object)
                        queue.Enqueue(item);

                    c++;
                    if (c >= 50) break;
                }

                continue;
            }

            if (node.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in node.EnumerateObject())
                {
                    if (prop.Value.ValueKind is JsonValueKind.Array or JsonValueKind.Object)
                        queue.Enqueue(prop.Value);
                }
            }
        }

        return false;
    }

    private static bool LooksLikeObjectArray(JsonElement arrayEl)
    {
        if (arrayEl.ValueKind != JsonValueKind.Array)
            return false;

        int inspected = 0;
        int objectCount = 0;
        foreach (var item in arrayEl.EnumerateArray())
        {
            inspected++;
            if (item.ValueKind == JsonValueKind.Object)
                objectCount++;

            if (inspected >= 10)
                break;
        }

        return inspected > 0 && objectCount > 0;
    }


}
