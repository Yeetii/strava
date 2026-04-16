using System.Globalization;
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

        if (DateOnly.TryParse(trimmed, CultureInfo.InvariantCulture, out var dateOnly))
            return dateOnly.ToString("yyyy-MM-dd");

        if (DateTime.TryParse(trimmed, CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var dt))
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
    // (e.g. "34.2 km, 12.9 km, Marathon") within a 25% relative tolerance. Returns the formatted match
    // (e.g. "34.2 km" or "42 km") or null when no distance list was provided or no close match is found.
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
            double km;
            string formatted;

            if (TryParseMarathonKeyword(part, out var marathonKm))
            {
                km = marathonKm;
                formatted = FormatDistanceKm(km);
            }
            else
            {
                var stripped = DistanceSuffixRegex().Replace(part, "").Trim();
                var suffix = DistanceSuffixRegex().Match(part).Value.ToLowerInvariant();

                if (!double.TryParse(stripped, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
                    continue;

                km = suffix == "mi" ? value * 1.60934 : value;
                formatted = FormatDistanceKm(km);
            }

            var delta = Math.Abs(km - distanceKm);
            if (delta < bestDelta)
            {
                bestDelta = delta;
                bestToken = formatted;
            }
        }

        if (bestToken is null)
            return null;

        // Accept the match only if it is within 25% of the GPX distance.
        var tolerance = distanceKm * 0.25;
        return bestDelta <= tolerance ? bestToken : null;
    }

    // Normalises a verbose distance string (e.g. "100K, 50K, Marathon") to the standard form ("100 km, 50 km, 42 km").
    // Recognises "marathon" → 42 km, "halvmarathon" / "half marathon" → 21 km.
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
            if (TryParseMarathonKeyword(part, out var marathonKm))
            {
                formatted.Add(FormatDistanceKm(marathonKm));
                continue;
            }

            // Strip trailing 'K', 'KM', 'km', 'm', 'mi' (case-insensitive) before numeric parse
            var stripped = DistanceSuffixRegex().Replace(part, "").Trim();
            var suffix = DistanceSuffixRegex().Match(part).Value.ToLowerInvariant();

            if (double.TryParse(stripped, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
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
            try { return new RegionInfo(upper).TwoLetterISORegionName; }
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

    // Derives a Cosmos-safe feature ID from a URL.
    // Strips "www." prefix, replaces path separators with "-".
    // e.g. https://julianalps.utmb.world/races/120K → "julianalps.utmb.world-races-120K"
    // e.g. https://www.vmxtreme.se/ → "vmxtreme.se"
    // e.g. https://tracedetrail.fr/trace/getTraceItra/12345 → "tracedetrail.fr-trace-getTraceItra-12345"
    // Appends "-{routeIndex}" suffix when multiple routes share the same URL.
    public static string BuildFeatureId(Uri url, int? routeIndex = null)
    {
        var host = url.Host;
        if (host.StartsWith("www.", StringComparison.OrdinalIgnoreCase))
            host = host[4..];

        var path = url.AbsolutePath.Trim('/');
        var id = string.IsNullOrEmpty(path)
            ? host
            : $"{host}-{path.Replace('/', '-')}";

        if (routeIndex.HasValue)
            id = $"{id}-{routeIndex.Value}";

        return id;
    }

    // Derives a Cosmos-safe feature ID from a name and optional distance string.
    // Sanitises each part to lowercase slug form (non-alphanumeric chars → "-").
    // Used as a fallback when no URL is available (e.g. Loppkartan point-only entries).
    public static string BuildFeatureId(string? name, string? distance)
    {
        var parts = new[] { name, distance }
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => NonSlugCharsRegex().Replace(s!.ToLowerInvariant(), "-").Trim('-'))
            .Where(s => s.Length > 0);
        return string.Join("-", parts);
    }

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

            double? distanceKm = null;
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
                        distanceKm = parsed;
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
            var distance = distanceKm.HasValue ? FormatDistanceKm(distanceKm.Value) : null;

            jobs.Add(new ScrapeJob(UtmbUrl: pageUri, Name: name, Distance: distance, ElevationGain: elevationGain,
                Country: country, Location: location,
                RaceType: "trail", ImageUrl: imageUrl, Playgrounds: playgrounds, RunningStones: runningStones));
        }

        return jobs
            .GroupBy(j => j.UtmbUrl!.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .Select(g => g.First())
            .ToList();
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
            var sports = FindStringValue(evt, ["sports", "sport"]);

            var imageBaseUrl = "https://tracedetrail.fr/events/";
            var imageName = FindStringValue(evt, ["img", "image", "imageUrl"]);
            var logoName = FindStringValue(evt, ["logo"]);
            var imageUrl = !string.IsNullOrWhiteSpace(imageName) ? $"{imageBaseUrl}{imageName}"
                         : !string.IsNullOrWhiteSpace(logoName) ? $"{imageBaseUrl}{logoName}"
                         : null;

            string[]? distanceParts = null;
            if (TryGetPropertyIgnoreCase(evt, "distances", out var distancesEl) && distancesEl.ValueKind == JsonValueKind.String)
                distanceParts = distancesEl.GetString()?.Split('_', StringSplitOptions.RemoveEmptyEntries);

            Uri? eventUrl = null;
            if (!string.IsNullOrWhiteSpace(slug))
                eventUrl = new Uri($"https://tracedetrail.fr/en/event/{slug}");

            for (int i = 0; i < traceIds.Length; i++)
            {
                if (!int.TryParse(traceIds[i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var traceId))
                    continue;

                string? distanceFormatted = null;
                if (distanceParts != null && i < distanceParts.Length &&
                    double.TryParse(distanceParts[i], NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                    distanceFormatted = FormatDistanceKm(d);

                var itraUrl = new Uri($"https://tracedetrail.fr/trace/getTraceItra/{traceId}");
                jobs.Add(new ScrapeJob(
                    TraceDeTrailItraUrl: itraUrl,
                    TraceDeTrailEventUrl: eventUrl,
                    Name: name,
                    Distance: distanceFormatted,
                    Country: country,
                    RaceType: sports,
                    ImageUrl: imageUrl));
            }
        }

        return jobs;
    }

    // Returns true if the token is a marathon keyword and sets km to the corresponding distance.
    // "marathon" → 42 km, "halvmarathon" / "half marathon" / "half-marathon" → 21 km.
    private static bool TryParseMarathonKeyword(string token, out double km)
    {
        if (token.Equals("marathon", StringComparison.OrdinalIgnoreCase))
        {
            km = 42.0;
            return true;
        }

        if (token.Equals("halvmarathon", StringComparison.OrdinalIgnoreCase) ||
            token.Equals("half marathon", StringComparison.OrdinalIgnoreCase) ||
            token.Equals("half-marathon", StringComparison.OrdinalIgnoreCase))
        {
            km = 21.0;
            return true;
        }

        km = 0;
        return false;
    }

    // Parses a verbose distance string into a list of (km, formatted) pairs.
    // Marathon keywords are translated; non-parseable tokens are skipped.
    private static IReadOnlyList<(double Km, string Formatted)> ParseVerboseDistanceParts(string distanceVerbose)
    {
        var parts = distanceVerbose
            .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var result = new List<(double, string)>(parts.Length);
        foreach (var part in parts)
        {
            if (TryParseMarathonKeyword(part, out var marathonKm))
            {
                result.Add((marathonKm, FormatDistanceKm(marathonKm)));
                continue;
            }

            var stripped = DistanceSuffixRegex().Replace(part, "").Trim();
            var suffix = DistanceSuffixRegex().Match(part).Value.ToLowerInvariant();

            if (double.TryParse(stripped, NumberStyles.Float, CultureInfo.InvariantCulture, out var value))
            {
                var km = suffix == "mi" ? value * 1.60934 : value;
                result.Add((km, FormatDistanceKm(km)));
            }
        }

        return result;
    }

    // Assigns verbose distances to GPX routes.
    //
    // Step 1 – primary matching: each verbose distance is matched to the closest route whose
    //   computed distance is within 25% tolerance. The first/closest match wins.
    // Step 2 – overflow: verbose distances that did not find a primary match are appended to the
    //   assignment list of the absolutely closest route (no tolerance restriction).
    //
    // Returns one list per route; the first element in each sub-list is the primary distance.
    public static IReadOnlyList<IReadOnlyList<string>> AssignDistancesToRoutes(
        IReadOnlyList<double> routeDistancesKm,
        string? distanceVerbose)
    {
        var assignments = Enumerable.Range(0, routeDistancesKm.Count)
            .Select(_ => new List<string>())
            .ToList();

        if (routeDistancesKm.Count == 0 || string.IsNullOrWhiteSpace(distanceVerbose))
            return assignments.Cast<IReadOnlyList<string>>().ToList();

        var verboseParts = ParseVerboseDistanceParts(distanceVerbose);
        if (verboseParts.Count == 0)
            return assignments.Cast<IReadOnlyList<string>>().ToList();

        var matched = new bool[verboseParts.Count];

        // Step 1: primary matching within 25% tolerance
        for (int j = 0; j < verboseParts.Count; j++)
        {
            var (verboseKm, verboseFormatted) = verboseParts[j];
            int bestIdx = -1;
            double bestDelta = double.MaxValue;

            for (int i = 0; i < routeDistancesKm.Count; i++)
            {
                var delta = Math.Abs(routeDistancesKm[i] - verboseKm);
                if (delta <= verboseKm * 0.25 && delta < bestDelta)
                {
                    bestDelta = delta;
                    bestIdx = i;
                }
            }

            if (bestIdx >= 0)
            {
                assignments[bestIdx].Add(verboseFormatted);
                matched[j] = true;
            }
        }

        // Step 2: assign unmatched verbose distances to the closest route (no tolerance)
        for (int j = 0; j < verboseParts.Count; j++)
        {
            if (matched[j]) continue;

            var (verboseKm, verboseFormatted) = verboseParts[j];
            int bestIdx = 0;
            double bestDelta = double.MaxValue;

            for (int i = 0; i < routeDistancesKm.Count; i++)
            {
                var delta = Math.Abs(routeDistancesKm[i] - verboseKm);
                if (delta < bestDelta)
                {
                    bestDelta = delta;
                    bestIdx = i;
                }
            }

            assignments[bestIdx].Add(verboseFormatted);
        }

        return assignments.Cast<IReadOnlyList<string>>().ToList();
    }

    // Matches one or more characters that are not Unicode letters, digits, or hyphens.
    // Used to sanitise names/distances into URL slug form.
    [GeneratedRegex(@"[^\p{L}\p{N}]+")]
    private static partial Regex NonSlugCharsRegex();
}

// Flat message enqueued onto the unified scrapeRace service bus queue.
// Each source has its own typed URL field; the worker tries scrapers in priority order
// (UTMB → ITRA/TraceDeTrail → Runagain → BFS/Loppkartan) and uses the first result.
// Any combination of source URLs may be set; all null → point fallback using lat/lng.
public record ScrapeJob(
    string? Name = null,
    string? Distance = null,     // pre-formatted, e.g. "50 km" or "50 km, 25 km"
    double? ElevationGain = null,
    string? Country = null,
    string? Location = null,
    string? RaceType = null,
    string? Date = null,
    string? ImageUrl = null,
    double? Latitude = null,
    double? Longitude = null,
    IReadOnlyList<string>? Playgrounds = null,
    IReadOnlyList<string>? RunningStones = null,
    string? County = null,
    string? TypeLocal = null,
    // Per-source URLs (null = source not available for this race).
    Uri? UtmbUrl = null,
    Uri? TraceDeTrailItraUrl = null,
    Uri? TraceDeTrailEventUrl = null,
    Uri? RunagainUrl = null,
    Uri? WebsiteUrl = null);         // generic race website (e.g. from Loppkartan)

public record TraceDeTrailTraceData(
    IReadOnlyList<(double Lng, double Lat)> Points,
    double? TotalDistanceKm,
    double? ElevationGain);
