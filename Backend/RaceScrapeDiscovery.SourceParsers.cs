using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Shared.Services;

namespace Backend;

public static partial class RaceScrapeDiscovery
{
    // Parses the response from https://api.utmb.world/search/races?lang=en&limit=400
    // Each race object has:
    //   slug            – the race page URL, e.g. "https://julianalps.utmb.world/races/120K"
    //   name            – race name
    //   details.statsUp – array of { name, value, postfix } where name in {"distance","elevationGain"}
    //   playgrounds     – array of playground objects (name of each UTMB World Series location)
    //   runningStones   – array of running stone objects
    //   image / imageUrl / thumbnail – optional image URL
    public static IReadOnlyCollection<ScrapeJob> ParseUtmbRacePages(string jsonPayload)
    {
        if (string.IsNullOrWhiteSpace(jsonPayload))
            return [];

        using var document = JsonDocument.Parse(jsonPayload);
        var root = document.RootElement;

        if (!TryGetPropertyIgnoreCase(root, "races", out var racesElement) || racesElement.ValueKind != JsonValueKind.Array)
            return [];

        var jobs = new List<ScrapeJob>();

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

            // Extract external ID
            IReadOnlyDictionary<string, string>? externalIds = null;
            if (TryGetPropertyIgnoreCase(race, "id", out var idEl))
            {
                var idStr = idEl.ValueKind == JsonValueKind.Number
                    ? idEl.GetRawText()
                    : idEl.ValueKind == JsonValueKind.String ? idEl.GetString() : null;
                if (!string.IsNullOrWhiteSpace(idStr))
                    externalIds = new Dictionary<string, string> { ["utmb"] = idStr! };
            }

            // Extract and normalise date
            var startDate = FindStringValue(race, ["startDate", "start_date", "date"]);
            var date = NormalizeDateToYyyyMmDd(startDate);

            // Extract country and location from "City, Country" in startLocation
            var startLocation = FindStringValue(race, ["startLocation", "start_location"]);
            string? country = null;
            string? location = null;
            if (!string.IsNullOrWhiteSpace(startLocation))
            {
                var locParts = startLocation.Split(',', 2, StringSplitOptions.TrimEntries);
                if (locParts.Length >= 2)
                {
                    location = locParts[0];
                    country = NormalizeCountryToIso2(locParts[^1]);
                }
                else
                {
                    location = locParts[0];
                }
            }
            country ??= NormalizeCountryToIso2(FindStringValue(race, ["country", "countryCode", "country_code"]));
            location ??= FindStringValue(race, ["city", "location", "venue", "cityName"]);

            // Extract registration status
            bool? registrationOpen = null;
            if (TryGetPropertyIgnoreCase(race, "raceStatus", out var raceStatus) && raceStatus.ValueKind == JsonValueKind.Object
                && TryGetPropertyIgnoreCase(raceStatus, "open", out var openEl))
            {
                registrationOpen = openEl.ValueKind switch
                {
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    _ => null
                };
            }

            double? distanceKm = null;
            double? elevationGain = null;
            int? runningStones = null;
            string? utmbWorldSeriesCategory = null;

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
                        distanceKm = parsed;
                    else if (string.Equals(key, "elevationGain", StringComparison.OrdinalIgnoreCase))
                        elevationGain = parsed;
                    else if (string.Equals(key, "runningStones", StringComparison.OrdinalIgnoreCase))
                        runningStones = statValue.ValueKind == JsonValueKind.Number && statValue.TryGetInt32(out var s) ? s : null;
                    else if (string.Equals(key, "categoryWorldSeries", StringComparison.OrdinalIgnoreCase))
                        utmbWorldSeriesCategory = statValue.ValueKind == JsonValueKind.String ? statValue.GetString() : null;
                }
            }

            // Extract playgrounds (UTMB World Series event groups)
            IReadOnlyList<string>? playgrounds = null;
            if (TryGetPropertyIgnoreCase(race, "playgrounds", out var playgroundsEl) && playgroundsEl.ValueKind == JsonValueKind.Array)
            {
                var names = new List<string>();
                foreach (var pg in playgroundsEl.EnumerateArray())
                {
                    var pgName = pg.ValueKind == JsonValueKind.String
                        ? pg.GetString()
                        : pg.ValueKind == JsonValueKind.Object ? FindStringValue(pg, ["name", "title", "slug"]) : null;
                    if (!string.IsNullOrWhiteSpace(pgName))
                        names.Add(pgName!);
                }
                if (names.Count > 0)
                    playgrounds = names;
            }

            // Extract image URL from media (Cloudinary)
            string? imageUrl = null;
            if (TryGetPropertyIgnoreCase(race, "media", out var mediaEl) && mediaEl.ValueKind == JsonValueKind.Object)
            {
                var publicId = FindStringValue(mediaEl, ["publicId"]);
                if (!string.IsNullOrWhiteSpace(publicId))
                    imageUrl = $"https://res.cloudinary.com/utmb-world/image/upload/{publicId.TrimStart('/')}";
            }

            // Extract logo URL from eventLogo (Cloudinary)
            string? logoUrl = null;
            if (TryGetPropertyIgnoreCase(race, "eventLogo", out var logoEl) && logoEl.ValueKind == JsonValueKind.Object)
            {
                var publicId = FindStringValue(logoEl, ["publicId"]);
                if (!string.IsNullOrWhiteSpace(publicId))
                    logoUrl = $"https://res.cloudinary.com/utmb-world/image/upload/{publicId.TrimStart('/')}";
            }

            var distance = distanceKm.HasValue ? FormatDistanceKm(distanceKm.Value) : null;

            jobs.Add(new ScrapeJob(UtmbUrl: pageUri, Name: name, ExternalIds: externalIds,
                Distance: distance, ElevationGain: elevationGain, Date: date,
                Country: country, Location: location, RegistrationOpen: registrationOpen,
                RaceType: "trail", ImageUrl: imageUrl, LogoUrl: logoUrl,
                Playgrounds: playgrounds, RunningStones: runningStones,
                UtmbWorldSeriesCategory: utmbWorldSeriesCategory));
        }

        return [.. jobs
            .GroupBy(j => j.UtmbUrl!.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())];
    }

    // Parses the response from https://www.loppkartan.se/markers-se.json
    // Response shape: { generatedAt, country, markers: [{ id, name, latitude, longitude, ... }] }
    public static IReadOnlyCollection<ScrapeJob> ParseLoppkartanMarkers(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object)
            return [];

        if (!TryGetPropertyIgnoreCase(root, "markers", out var markersEl) || markersEl.ValueKind != JsonValueKind.Array)
            return [];

        var jobsByMarkerId = new Dictionary<string, ScrapeJob>(StringComparer.OrdinalIgnoreCase);
        foreach (var marker in markersEl.EnumerateArray())
        {
            if (marker.ValueKind != JsonValueKind.Object)
                continue;

            var markerId = FindStringValue(marker, ["id"]);
            if (string.IsNullOrWhiteSpace(markerId))
                continue;

            if (jobsByMarkerId.ContainsKey(markerId))
                continue;

            if (!TryGetDoubleValue(marker, "latitude", out var latitude))
                continue;
            if (!TryGetDoubleValue(marker, "longitude", out var longitude))
                continue;

            var website = FindStringValue(marker, ["website"]);
            Uri? websiteUri = null;
            if (!string.IsNullOrWhiteSpace(website) &&
                Uri.TryCreate(website, UriKind.Absolute, out var parsed) &&
                parsed.Scheme is "http" or "https")
                websiteUri = parsed;

            var distanceVerbose = FindStringValue(marker, ["distance_verbose"]);
            var distance = ParseDistanceVerbose(distanceVerbose);

            jobsByMarkerId[markerId] = new ScrapeJob(
                WebsiteUrl: websiteUri,
                Name: FindStringValue(marker, ["name"]),
                Distance: distance,
                Latitude: latitude,
                Longitude: longitude,
                Location: FindStringValue(marker, ["location"]),
                County: FindStringValue(marker, ["county"]),
                Date: FindStringValue(marker, ["race_date"]),
                RaceType: FindStringValue(marker, ["race_type"]),
                TypeLocal: FindStringValue(marker, ["type_local"]),
                Country: FindStringValue(marker, ["origin_country"]));
        }

        return jobsByMarkerId.Values.ToList();
    }

    public static IReadOnlyCollection<ScrapeJob> ParseDuvCalendarPage(string html, Uri baseUrl)
        => DuvDiscoveryAgent.ParseCalendarPage(html, baseUrl);

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
            && double.TryParse(property.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out value))
            return true;

        return false;
    }

    // Parses the response from POST https://tracedetrail.fr/event/getEventsCalendar/all/all/all
    // Each event has traceIDs (underscore-separated ints), distances (underscore-separated km values), nom
    // Returns one ScrapeJob per trace ID.  Each job carries:
    //   TraceDeTrailItraUrl  – ITRA JSON endpoint for the specific trace (primary route source)
    //   TraceDeTrailEventUrl – event page used as fallback when the ITRA endpoint yields nothing
    public static IReadOnlyCollection<ScrapeJob> ParseTraceDeTrailCalendarEvents(string json)
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

        var jobs = new List<ScrapeJob>();

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
            var country = FindStringValue(evt, ["country", "pays", "countryCode"]);
            var slug = FindStringValue(evt, ["label"]);
            // If slug looks like a path (e.g. www.sormlands100.com or foo/bar), only use the last segment
            if (!string.IsNullOrWhiteSpace(slug) && slug.Contains('/'))
            {
                var parts = slug.Split('/', StringSplitOptions.RemoveEmptyEntries);
                slug = parts.Length > 0 ? parts[^1] : slug;
            }
            var sports = NormalizeRaceType(FindStringValue(evt, ["sports", "sport"]));
            var date = NormalizeDateToYyyyMmDd(FindStringValue(evt, ["dateDeb", "date", "startDate"]));

            // Build ExternalIds from evtID / itraEvtID
            Dictionary<string, string>? externalIds = null;
            var evtId = FindStringValue(evt, ["evtID"]);
            var itraEvtId = FindStringValue(evt, ["itraEvtID"]);
            if (!string.IsNullOrWhiteSpace(evtId) || !string.IsNullOrWhiteSpace(itraEvtId))
            {
                externalIds = new(StringComparer.Ordinal);
                if (!string.IsNullOrWhiteSpace(evtId)) externalIds["tracedetrailEventId"] = evtId!;
                if (!string.IsNullOrWhiteSpace(itraEvtId)) externalIds["itraEventId"] = itraEvtId!;
            }

            var imageBaseUrl = "https://tracedetrail.fr/events/";
            var imageName = FindStringValue(evt, ["img", "image", "imageUrl"]);
            var logoName = FindStringValue(evt, ["logo"]);
            var imageUrl = !string.IsNullOrWhiteSpace(imageName) ? $"{imageBaseUrl}{imageName}" : null;
            var logoUrl = !string.IsNullOrWhiteSpace(logoName) ? $"{imageBaseUrl}{logoName}" : null;
            // Fallback: if no image, use logo as image
            imageUrl ??= logoUrl;

            string[]? distances = null;
            if (TryGetPropertyIgnoreCase(evt, "distances", out var distancesEl) && distancesEl.ValueKind == JsonValueKind.String)
                distances = distancesEl.GetString()?.Split('_', StringSplitOptions.RemoveEmptyEntries);


            Uri? eventUrl = null;
            Uri? websiteUrl = null;
            if (!string.IsNullOrWhiteSpace(slug))
            {
                // If slug looks like a domain or URL, treat as external event website
                if (Regex.IsMatch(slug, @"^[\w.-]+\.[a-z]{2,}(\/.*)?$", RegexOptions.IgnoreCase))
                {
                    // Prepend https:// if missing
                    if (!slug.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
                        !slug.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                    {
                        websiteUrl = new Uri($"https://{slug}");
                    }
                    else
                    {
                        websiteUrl = new Uri(slug);
                    }
                }
                else
                {
                    eventUrl = new Uri($"https://tracedetrail.fr/en/event/{slug}");
                }
            }

            // Build all ITRA URLs and combine distances
            var itraUrls = new List<Uri>();
            var distanceParts = new List<string>();

            for (int i = 0; i < traceIds.Length; i++)
            {
                if (!int.TryParse(traceIds[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var traceId))
                    continue;

                itraUrls.Add(new Uri($"https://tracedetrail.fr/trace/getTraceItra/{traceId}"));

                if (distances != null && i < distances.Length &&
                    double.TryParse(distances[i], NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                    distanceParts.Add(FormatDistanceKm(d));
            }

            if (itraUrls.Count == 0)
                continue;

            var distance = distanceParts.Count > 0 ? string.Join(", ", distanceParts) : null;

            jobs.Add(new ScrapeJob(
                TraceDeTrailItraUrls: itraUrls,
                TraceDeTrailEventUrl: eventUrl,
                WebsiteUrl: websiteUrl,
                Name: name,
                ExternalIds: externalIds,
                Distance: distance,
                Country: country,
                Date: date,
                RaceType: sports,
                ImageUrl: imageUrl,
                LogoUrl: logoUrl));
        }

        return jobs;
    }

    // Parses the response from POST https://cloudrun-pgjjiy2k6a-ew.a.run.app/find_runs
    // Response: {"hits":[...],"estimatedTotalHits":N,"offset":N,"last_item":bool}
    // Each hit has post_title, post_url, country, place, county, date, gps, length[], race_type[], terrain_type[], cover_image, race_guid.
    public static IReadOnlyCollection<ScrapeJob> ParseRunagainSearchResults(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!TryGetPropertyIgnoreCase(root, "hits", out var hitsEl) || hitsEl.ValueKind != JsonValueKind.Array)
            return [];

        var jobs = new List<ScrapeJob>();

        foreach (var hit in hitsEl.EnumerateArray())
        {
            if (hit.ValueKind != JsonValueKind.Object)
                continue;

            var slug = FindStringValue(hit, ["post_url"]);
            if (string.IsNullOrWhiteSpace(slug))
                continue;

            var eventUrl = new Uri($"https://runagain.com/find-event/{slug}");
            var name = FindStringValue(hit, ["post_title"]);
            var country = FindStringValue(hit, ["country"]);
            var location = FindStringValue(hit, ["place"]);
            var county = FindStringValue(hit, ["county"]);
            var date = NormalizeDateToYyyyMmDd(FindStringValue(hit, ["date"]));
            var imageUrl = FindStringValue(hit, ["cover_image"]);
            if (string.IsNullOrWhiteSpace(imageUrl)) imageUrl = null;

            // race_type and terrain_type are arrays of strings
            string? raceType = null;
            if (TryGetPropertyIgnoreCase(hit, "race_type", out var raceTypeEl) && raceTypeEl.ValueKind == JsonValueKind.Array)
            {
                var types = new List<string>();
                foreach (var t in raceTypeEl.EnumerateArray())
                    if (t.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(t.GetString()))
                        types.Add(t.GetString()!);
                if (TryGetPropertyIgnoreCase(hit, "terrain_type", out var terrainEl) && terrainEl.ValueKind == JsonValueKind.Array)
                    foreach (var t in terrainEl.EnumerateArray())
                        if (t.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(t.GetString()))
                            types.Add(t.GetString()!);
                raceType = NormalizeRaceType(string.Join(", ", types));
            }

            // TypeLocal stores the original Norwegian race_type terms (e.g. "Stiløp")
            string? typeLocal = null;
            if (TryGetPropertyIgnoreCase(hit, "race_type", out var raceTypeLocalEl) && raceTypeLocalEl.ValueKind == JsonValueKind.Array)
            {
                var localTypes = new List<string>();
                foreach (var t in raceTypeLocalEl.EnumerateArray())
                    if (t.ValueKind == JsonValueKind.String && !string.IsNullOrWhiteSpace(t.GetString()))
                        localTypes.Add(t.GetString()!);
                typeLocal = localTypes.Count > 0 ? string.Join(", ", localTypes) : null;
            }

            // length is an array of distances in km
            string? distance = null;
            if (TryGetPropertyIgnoreCase(hit, "length", out var lengthEl) && lengthEl.ValueKind == JsonValueKind.Array)
            {
                var parts = new List<string>();
                foreach (var l in lengthEl.EnumerateArray())
                    if (l.TryGetDouble(out var d) && d > 0)
                        parts.Add(FormatDistanceKm(d));
                distance = parts.Count > 0 ? string.Join(", ", parts) : null;
            }

            // gps is [lat, lng]
            double? lat = null, lng = null;
            if (TryGetPropertyIgnoreCase(hit, "gps", out var gpsEl) && gpsEl.ValueKind == JsonValueKind.Array)
            {
                var coords = new List<double>();
                foreach (var c in gpsEl.EnumerateArray())
                    if (c.TryGetDouble(out var v))
                        coords.Add(v);
                if (coords.Count >= 2)
                {
                    lat = coords[0];
                    lng = coords[1];
                }
            }

            // ExternalIds from race_guid
            Dictionary<string, string>? externalIds = null;
            var guid = FindStringValue(hit, ["race_guid"]);
            if (!string.IsNullOrWhiteSpace(guid))
                externalIds = new(StringComparer.Ordinal) { ["runagain"] = guid! };

            jobs.Add(new ScrapeJob(
                RunagainUrl: eventUrl,
                Name: name,
                ExternalIds: externalIds,
                Distance: distance,
                Country: country,
                Location: location,
                County: county,
                Date: date,
                RaceType: raceType,
                TypeLocal: typeLocal,
                ImageUrl: imageUrl,
                Latitude: lat,
                Longitude: lng));
        }

        return jobs;
    }

}
