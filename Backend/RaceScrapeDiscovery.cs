using System.Text.Json;
using System.Text.RegularExpressions;

namespace Backend;

public static partial class RaceScrapeDiscovery
{
    public const string LastScrapedUtcProperty = "lastScrapedUtc";

    // Shared Cosmos property name constants — use these in all race upsert workers.
    public const string PropName = "name";
    public const string PropDate = "date";
    public const string PropWebsite = "website";
    public const string PropDistance = "distance";
    public const string PropElevationGain = "elevationGain";
    public const string PropRaceType = "raceType";
    public const string PropDescription = "description";
    public const string PropCountry = "country";
    public const string PropLocation = "location";
    public const string PropImage = "image";
    public const string PropPlaygrounds = "playgrounds";
    public const string PropRunningStones = "runningStones";

    // Converts various date formats to YYYY-MM-DD, e.g. "20250914" → "2025-09-14".
    // Returns null if the date cannot be parsed.
    public static string? NormalizeDateToYyyyMmDd(string? date)
    {
        if (string.IsNullOrWhiteSpace(date))
            return null;

        var trimmed = date.Trim();

        // Already YYYY-MM-DD
        if (trimmed.Length == 10 && trimmed[4] == '-' && trimmed[7] == '-')
            return trimmed;

        // Compact YYYYMMDD (e.g. Loppkartan: "20250914")
        if (trimmed.Length == 8 && trimmed.All(char.IsDigit))
            return $"{trimmed[..4]}-{trimmed[4..6]}-{trimmed[6..8]}";

        if (DateOnly.TryParse(trimmed, System.Globalization.CultureInfo.InvariantCulture, out var dateOnly))
            return dateOnly.ToString("yyyy-MM-dd");

        if (DateTime.TryParse(trimmed, System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.None, out var dt))
            return dt.ToString("yyyy-MM-dd");

        return null;
    }

    // Formats a numeric distance in km to a human-readable string, e.g. 10.1 → "10.1 km", 5.0 → "5 km".
    public static string FormatDistanceKm(double distanceKm)
    {
        return distanceKm == Math.Floor(distanceKm)
            ? $"{(long)distanceKm} km"
            : $"{distanceKm:0.#} km";
    }

    // Matches a computed GPX distance (km) to the closest entry in a verbose distance string
    // (e.g. "34.2 km, 12.9 km") within a 25% relative tolerance. Returns the formatted match
    // (e.g. "34.2 km") or null when no distance list was provided or no close match is found.
    public static string? MatchDistanceKmToVerbose(double distanceKm, string? distanceVerbose)
    {
        if (string.IsNullOrWhiteSpace(distanceVerbose) || distanceKm <= 0)
            return null;

        var parts = distanceVerbose
            .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        string? bestToken = null;
        double bestDelta = double.MaxValue;

        foreach (var part in parts)
        {
            var stripped = DistanceSuffixRegex().Replace(part, "").Trim();
            var suffix = DistanceSuffixRegex().Match(part).Value.ToLowerInvariant();

            if (!double.TryParse(stripped, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var value))
                continue;

            var km = suffix == "mi" ? value * 1.60934 : value;
            var delta = Math.Abs(km - distanceKm);
            if (delta < bestDelta)
            {
                bestDelta = delta;
                bestToken = FormatDistanceKm(km);
            }
        }

        if (bestToken is null)
            return null;

        // Accept the match only if it is within 25% of the GPX distance.
        var tolerance = distanceKm * 0.25;
        return bestDelta <= tolerance ? bestToken : null;
    }

    // Normalises a verbose distance string (e.g. "100K, 50K") to the standard form ("100 km, 50 km").
    // Individual tokens that cannot be parsed are passed through unchanged.
    public static string? ParseDistanceVerbose(string? distanceVerbose)
    {
        if (string.IsNullOrWhiteSpace(distanceVerbose))
            return null;

        var parts = distanceVerbose
            .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var formatted = new List<string>(parts.Length);
        foreach (var part in parts)
        {
            // Strip trailing 'K', 'KM', 'km', 'm', 'mi' (case-insensitive) before numeric parse
            var stripped = DistanceSuffixRegex().Replace(part, "").Trim();
            var suffix = DistanceSuffixRegex().Match(part).Value.ToLowerInvariant();

            if (double.TryParse(stripped, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var value))
            {
                // Treat "mi" as miles → convert to km
                var km = suffix == "mi" ? value * 1.60934 : value;
                formatted.Add(FormatDistanceKm(km));
            }
            else
            {
                formatted.Add(part); // pass through as-is
            }
        }

        return string.Join(", ", formatted);
    }

    // Normalises a country string to an ISO 3166-1 alpha-2 code (e.g. "france" → "FR", "SWE" → "SE").
    // Returns null if the country cannot be identified.
    public static string? NormalizeCountryToIso2(string? country)
    {
        if (string.IsNullOrWhiteSpace(country))
            return null;

        var trimmed = country.Trim();

        if (trimmed.Length == 2)
        {
            var upper = trimmed.ToUpperInvariant();
            try { return new System.Globalization.RegionInfo(upper).TwoLetterISORegionName; }
            catch { return null; }
        }

        if (trimmed.Length == 3)
        {
            if (Iso3ToIso2.TryGetValue(trimmed.ToUpperInvariant(), out var from3))
                return from3;
        }

        if (CountryNameToIso2.TryGetValue(trimmed.ToLowerInvariant(), out var fromName))
            return fromName;

        return null;
    }

    // Normalises a race type string. If any segment contains the word "trail", the token "trail" is
    // prepended. All segments are lower-cased and deduplicated, then joined with ", ".
    // Returns null for blank input.
    public static string? NormalizeRaceType(string? raceType)
    {
        if (string.IsNullOrWhiteSpace(raceType))
            return null;

        var parts = raceType
            .Split([',', ';', '/'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var hasTrailToken = parts.Contains("trail", StringComparer.Ordinal);
        var hasTrailWord = !hasTrailToken && parts.Any(p => p.Contains("trail", StringComparison.OrdinalIgnoreCase));
        if (hasTrailWord)
        {
            parts.Insert(0, "trail");
        }

        return parts.Count > 0 ? string.Join(", ", parts) : null;
    }

    // ISO 3166-1 alpha-3 → alpha-2 mappings for common trail-running countries.
    private static readonly Dictionary<string, string> Iso3ToIso2 = new(StringComparer.OrdinalIgnoreCase)
    {
        ["AND"] = "AD", ["ALB"] = "AL", ["AUT"] = "AT", ["BEL"] = "BE", ["BGR"] = "BG",
        ["BIH"] = "BA", ["BLR"] = "BY", ["CHE"] = "CH", ["CYP"] = "CY", ["CZE"] = "CZ",
        ["DEU"] = "DE", ["DNK"] = "DK", ["ESP"] = "ES", ["EST"] = "EE", ["FIN"] = "FI",
        ["FRA"] = "FR", ["GBR"] = "GB", ["GRC"] = "GR", ["HRV"] = "HR", ["HUN"] = "HU",
        ["IRL"] = "IE", ["ISL"] = "IS", ["ITA"] = "IT", ["KOS"] = "XK", ["LIE"] = "LI",
        ["LTU"] = "LT", ["LUX"] = "LU", ["LVA"] = "LV", ["MDA"] = "MD", ["MKD"] = "MK",
        ["MLT"] = "MT", ["MNE"] = "ME", ["NLD"] = "NL", ["NOR"] = "NO", ["POL"] = "PL",
        ["PRT"] = "PT", ["ROU"] = "RO", ["RUS"] = "RU", ["SRB"] = "RS", ["SVK"] = "SK",
        ["SVN"] = "SI", ["SWE"] = "SE", ["TUR"] = "TR", ["UKR"] = "UA",
        ["AUS"] = "AU", ["BRA"] = "BR", ["CAN"] = "CA", ["CHN"] = "CN", ["HKG"] = "HK",
        ["IDN"] = "ID", ["IND"] = "IN", ["JPN"] = "JP", ["KOR"] = "KR", ["MEX"] = "MX",
        ["MYS"] = "MY", ["NZL"] = "NZ", ["PHL"] = "PH", ["SGP"] = "SG", ["THA"] = "TH",
        ["USA"] = "US", ["ZAF"] = "ZA",
    };

    // Common country name (lower-case) → ISO 3166-1 alpha-2 code.
    private static readonly Dictionary<string, string> CountryNameToIso2 = new(StringComparer.OrdinalIgnoreCase)
    {
        // English names
        ["andorra"] = "AD", ["albania"] = "AL", ["austria"] = "AT", ["belgium"] = "BE",
        ["bulgaria"] = "BG", ["bosnia and herzegovina"] = "BA", ["bosnia"] = "BA",
        ["belarus"] = "BY", ["switzerland"] = "CH", ["cyprus"] = "CY", ["czechia"] = "CZ",
        ["czech republic"] = "CZ", ["germany"] = "DE", ["denmark"] = "DK", ["spain"] = "ES",
        ["estonia"] = "EE", ["finland"] = "FI", ["france"] = "FR", ["united kingdom"] = "GB",
        ["uk"] = "GB", ["great britain"] = "GB", ["greece"] = "GR", ["croatia"] = "HR",
        ["hungary"] = "HU", ["ireland"] = "IE", ["iceland"] = "IS", ["italy"] = "IT",
        ["kosovo"] = "XK", ["liechtenstein"] = "LI", ["lithuania"] = "LT",
        ["luxembourg"] = "LU", ["latvia"] = "LV", ["moldova"] = "MD",
        ["north macedonia"] = "MK", ["malta"] = "MT", ["montenegro"] = "ME",
        ["netherlands"] = "NL", ["norway"] = "NO", ["poland"] = "PL", ["portugal"] = "PT",
        ["romania"] = "RO", ["russia"] = "RU", ["serbia"] = "RS", ["slovakia"] = "SK",
        ["slovenia"] = "SI", ["sweden"] = "SE", ["turkey"] = "TR", ["ukraine"] = "UA",
        ["australia"] = "AU", ["brazil"] = "BR", ["canada"] = "CA", ["china"] = "CN",
        ["hong kong"] = "HK", ["indonesia"] = "ID", ["india"] = "IN", ["japan"] = "JP",
        ["south korea"] = "KR", ["korea"] = "KR", ["mexico"] = "MX", ["malaysia"] = "MY",
        ["new zealand"] = "NZ", ["philippines"] = "PH", ["singapore"] = "SG",
        ["thailand"] = "TH", ["united states"] = "US", ["usa"] = "US",
        ["south africa"] = "ZA",
        // French names (TraceDeTrail is a French site)
        ["allemagne"] = "DE", ["autriche"] = "AT", ["belgique"] = "BE", ["bulgarie"] = "BG",
        ["croatie"] = "HR", ["danemark"] = "DK", ["espagne"] = "ES", ["finlande"] = "FI",
        ["grèce"] = "GR", ["grece"] = "GR", ["hongrie"] = "HU", ["irlande"] = "IE",
        ["islande"] = "IS", ["italie"] = "IT", ["lettonie"] = "LV", ["lituanie"] = "LT",
        ["macédoine du nord"] = "MK", ["macedoine du nord"] = "MK", ["malte"] = "MT",
        ["moldavie"] = "MD", ["monténégro"] = "ME", ["montenegro"] = "ME",
        ["norvège"] = "NO", ["norvege"] = "NO", ["pays-bas"] = "NL", ["pays bas"] = "NL",
        ["pologne"] = "PL", ["portugal"] = "PT", ["roumanie"] = "RO", ["russie"] = "RU",
        ["serbie"] = "RS", ["slovaquie"] = "SK", ["slovénie"] = "SI", ["slovenie"] = "SI",
        ["suède"] = "SE", ["suede"] = "SE", ["suisse"] = "CH", ["tchéquie"] = "CZ",
        ["tcheque"] = "CZ", ["turquie"] = "TR", ["ukraine"] = "UA",
        ["australie"] = "AU", ["brésil"] = "BR", ["bresil"] = "BR", ["canada"] = "CA",
        ["chine"] = "CN", ["japon"] = "JP", ["mexique"] = "MX", ["nouvelle-zélande"] = "NZ",
        ["nouvelle zelande"] = "NZ", ["afrique du sud"] = "ZA",
        ["royaume-uni"] = "GB", ["royaume uni"] = "GB",
        ["états-unis"] = "US", ["etats-unis"] = "US", ["etats unis"] = "US",
    };

    [GeneratedRegex(@"(?i)(km|k|mi|m)\s*$")]
    private static partial Regex DistanceSuffixRegex();

    // Derives a stable source-scoped ID from the UTMB race page URL.
    // e.g. https://julianalps.utmb.world/races/120K → "utmb:julianalps/120K"
    public static string BuildUtmbFeatureId(Uri coursePageUrl)
    {
        var host = coursePageUrl.Host;
        const string utmbSuffix = ".utmb.world";
        var subdomain = host.EndsWith(utmbSuffix, StringComparison.OrdinalIgnoreCase)
            ? host[..^utmbSuffix.Length]
            : host;
        var path = coursePageUrl.AbsolutePath.Trim('/');
        return $"utmb:{subdomain}/{path}";
    }

    // Parses the response from https://api.utmb.world/search/races?lang=en&limit=400
    // Each race object has:
    //   slug            – the race page URL, e.g. "https://julianalps.utmb.world/races/120K"
    //   name            – race name
    //   details.statsUp – array of { name, value, postfix } where name in {"distance","elevationGain"}
    //   playgrounds     – array of playground objects (name of each UTMB World Series location)
    //   runningStones   – array of running stone objects
    //   image / imageUrl / thumbnail – optional image URL
    public static IReadOnlyCollection<RacePageCandidate> ParseUtmbRacePages(string jsonPayload)
    {
        if (string.IsNullOrWhiteSpace(jsonPayload))
            return [];

        using var document = JsonDocument.Parse(jsonPayload);
        var root = document.RootElement;

        if (!TryGetPropertyIgnoreCase(root, "races", out var racesElement) || racesElement.ValueKind != JsonValueKind.Array)
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

            var country = FindStringValue(race, ["country", "countryCode", "country_code"]);
            var location = FindStringValue(race, ["city", "location", "venue", "cityName"]);

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

            // Extract running stones
            IReadOnlyList<string>? runningStones = null;
            if (TryGetPropertyIgnoreCase(race, "runningStones", out var stonesEl) && stonesEl.ValueKind == JsonValueKind.Array)
            {
                var stones = new List<string>();
                foreach (var stone in stonesEl.EnumerateArray())
                {
                    var stoneName = stone.ValueKind == JsonValueKind.String
                        ? stone.GetString()
                        : stone.ValueKind == JsonValueKind.Object ? FindStringValue(stone, ["name", "title", "slug"]) : null;
                    if (!string.IsNullOrWhiteSpace(stoneName))
                        stones.Add(stoneName!);
                }
                if (stones.Count > 0)
                    runningStones = stones;
            }

            // Extract image URL
            var imageUrl = FindStringValue(race, ["image", "imageUrl", "thumbnail", "picture"]);

            pageCandidates.Add(new RacePageCandidate(pageUri, name, distance, elevationGain, country, location, playgrounds, runningStones, imageUrl));
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

        var matches = HrefRegex().Matches(html).Select(m => m.Groups["href"].Value)
            .Concat(AbsoluteGpxRegex().Matches(html).Select(m => m.Groups["url"].Value))
            .Concat(RelativeGpxRegex().Matches(html).Select(m => m.Groups["url"].Value));

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
            var country = FindStringValue(evt, ["country", "pays", "countryCode"]);
            var slug = FindStringValue(evt, ["label",]);

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

                targets.Add(new TraceDeTrailScrapeTarget(traceId, name, distance, country, slug));
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

public record RacePageCandidate(
    Uri PageUrl,
    string? Name,
    double? Distance,
    double? ElevationGain,
    string? Country,
    string? Location,
    IReadOnlyList<string>? Playgrounds = null,
    IReadOnlyList<string>? RunningStones = null,
    string? ImageUrl = null);

public record RaceScrapeTarget(
    Uri GpxUrl,
    Uri SourceUrl,
    Uri CoursePageUrl,
    string? Name,
    double? Distance,
    double? ElevationGain,
    string? Country,
    string? Location,
    IReadOnlyList<string>? Playgrounds = null,
    IReadOnlyList<string>? RunningStones = null,
    string? ImageUrl = null);

public record TraceDeTrailScrapeTarget(int TraceId, string? Name, double? Distance, string? Country, string? Slug);

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
