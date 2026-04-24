using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Shared.Services;

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
    public const string PropItraPoints = "itraPoints";
    public const string PropItraNationalLeague = "itraNationalLeague";
    public const string PropSources = "sources";
    public const string PropLogo = "logo";
    public const string PropOrganizer = "organizer";
    public const string PropStartFee = "startFee";
    public const string PropCurrency = "currency";

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

        // Strip ordinal suffixes (1st, 2nd, 3rd, 4th, etc.) before parsing
        var cleaned = OrdinalSuffixRegex().Replace(trimmed, "$1");

        // Replace non-English month names with English equivalents for InvariantCulture parsing.
        cleaned = ReplaceLocalizedMonths(cleaned);
        // Strip Swedish time markers like "kl. 10:00" that appear after dates.
        cleaned = TimeSuffixRegex().Replace(cleaned, "");
        // Remove Swedish weekday prefixes like "söndag" or "lördag".
        cleaned = WeekdayRegex().Replace(cleaned, "");
        // Strip "den " prefix and trailing period after day number (e.g. "den 24. mai" → "24 may").
        cleaned = DenPrefixRegex().Replace(cleaned, "");
        cleaned = DayPeriodRegex().Replace(cleaned, "$1 ");

        var dottedMatch = DottedDateRegex().Match(cleaned);
        if (dottedMatch.Success)
        {
            var day = dottedMatch.Groups[1].Value.PadLeft(2, '0');
            var month = dottedMatch.Groups[2].Value.PadLeft(2, '0');
            var year = dottedMatch.Groups[3].Value;
            return $"{year}-{month}-{day}";
        }

        if (DateOnly.TryParse(cleaned, CultureInfo.InvariantCulture, out var dateOnly))
            return dateOnly.ToString("yyyy-MM-dd");

        var dateCandidate = PlainDateRegex.Match(cleaned).Value;
        if (!string.IsNullOrWhiteSpace(dateCandidate)
            && DateOnly.TryParse(dateCandidate, CultureInfo.InvariantCulture, out dateOnly))
        {
            return dateOnly.ToString("yyyy-MM-dd");
        }

        if (DateTime.TryParse(cleaned, CultureInfo.InvariantCulture,
            DateTimeStyles.None, out var dt))
            return dt.ToString("yyyy-MM-dd");

        return null;
    }

    public static string? NormalizeDuvCalendarDateToYyyyMmDd(string? date)
    {
        if (string.IsNullOrWhiteSpace(date))
            return null;

        var cleaned = WebUtility.HtmlDecode(date).Trim();

        var match = DuvDateRangeNoMonthRegex().Match(cleaned);
        if (match.Success)
        {
            var candidate = $"{match.Groups["startDay"].Value}.{match.Groups["month"].Value}.{match.Groups["year"].Value}";
            return NormalizeDateToYyyyMmDd(candidate);
        }

        match = DuvDateRangeMonthRegex().Match(cleaned);
        if (match.Success)
        {
            var candidate = $"{match.Groups["startDay"].Value}.{match.Groups["startMonth"].Value}.{match.Groups["year"].Value}";
            return NormalizeDateToYyyyMmDd(candidate);
        }

        match = DuvDateRangeFullRegex().Match(cleaned);
        if (match.Success)
        {
            var candidate = $"{match.Groups["startDay"].Value}.{match.Groups["startMonth"].Value}.{match.Groups["year"].Value}";
            return NormalizeDateToYyyyMmDd(candidate);
        }

        return NormalizeDateToYyyyMmDd(cleaned);
    }

    // Maps non-English month names/abbreviations to English for date parsing.
    private static readonly (string Pattern, string Replacement)[] MonthReplacements =
    [
        // Norwegian
        ("januar", "january"), ("februar", "february"), ("mars", "march"),
        ("april", "april"), ("mai", "may"), ("juni", "june"),
        ("juli", "july"), ("august", "august"), ("september", "september"),
        ("oktober", "october"), ("november", "november"), ("desember", "december"),
        // Swedish
        ("januari", "january"), ("februari", "february"), ("maj", "may"),
        ("augusti", "august"), ("december", "december"),
        // German
        ("märz", "march"), ("marz", "march"), ("juni", "june"),
        ("juli", "july"), ("oktober", "october"), ("dezember", "december"),
        // French
        ("janvier", "january"), ("février", "february"), ("fevrier", "february"),
        ("mars", "march"), ("avril", "april"), ("mai", "may"),
        ("juin", "june"), ("juillet", "july"), ("août", "august"), ("aout", "august"),
        ("septembre", "september"), ("octobre", "october"),
        ("novembre", "november"), ("décembre", "december"), ("decembre", "december"),
    ];

    private static string ReplaceLocalizedMonths(string input)
    {
        foreach (var (pattern, replacement) in MonthReplacements)
        {
            var idx = input.IndexOf(pattern, StringComparison.OrdinalIgnoreCase);
            if (idx >= 0)
                return string.Concat(input.AsSpan(0, idx), replacement, input.AsSpan(idx + pattern.Length));
        }
        return input;
    }

    [GeneratedRegex(@"\bden\s+", RegexOptions.IgnoreCase)]
    private static partial Regex DenPrefixRegex();

    [GeneratedRegex(@"kl\.?\s*\d{1,2}[:.]\d{2}", RegexOptions.IgnoreCase)]
    private static partial Regex TimeSuffixRegex();

    [GeneratedRegex(@"\b(?:måndag|tisdag|onsdag|torsdag|fredag|lördag|söndag|mandag|tirsdag|onsdag|torsdag|fredag|lordag|sondag)\b", RegexOptions.IgnoreCase)]
    private static partial Regex WeekdayRegex();

    private static readonly Regex PlainDateRegex = new(
        @"\b\d{1,2}\.?\s+(?:[A-Za-z]+\s+\d{4})\b|\b[A-Za-z]+\s+\d{1,2},?\s+\d{4}\b|\b\d{1,2}[/.]\d{1,2}[/.]\d{4}\b|\b\d{4}-\d{2}-\d{2}\b",
        RegexOptions.IgnoreCase);

    [GeneratedRegex(@"(\d)\.?\s+(?=[a-zA-Z])", RegexOptions.None)]
    private static partial Regex DayPeriodRegex();
    // Formats a numeric distance in km to a human-readable string, e.g. 10.1 → "10.1 km", 5.0 → "5 km".
    public static string FormatDistanceKm(double distanceKm)
    {
        return distanceKm == Math.Floor(distanceKm)
            ? $"{(long)distanceKm} km"
            : string.Create(CultureInfo.InvariantCulture, $"{distanceKm:0.#} km");
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
        double? bestMatchedKm = null;

        foreach (var part in parts)
        {
            if (!RaceDistanceKm.TryParseCommaListTokenKilometers(part, out var km))
                continue;

            var formatted = FormatDistanceKm(km);
            var delta = Math.Abs(km - distanceKm);
            if (delta < bestDelta)
            {
                bestDelta = delta;
                bestToken = formatted;
                bestMatchedKm = km;
            }
        }

        if (bestToken is null || bestMatchedKm is null)
            return null;

        return RaceDistanceKm.WithinRelativeOfReference(distanceKm, bestMatchedKm.Value, 0.25)
            ? bestToken
            : null;
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
            if (RaceDistanceKm.TryParseCommaListTokenKilometers(part, out var km))
                formatted.Add(FormatDistanceKm(km));
            else
                formatted.Add(part); // pass through as-is
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

    // Normalises a race type string. Splits on common separators, maps known Norwegian/Swedish/French
    // terms to English categories, deduplicates, and joins with ", ".
    // Returns null for blank input.
    public static string? NormalizeRaceType(string? raceType) => RaceTypeNormalizer.NormalizeRaceType(raceType);

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

    [GeneratedRegex(@"^\s*(\d{1,2})\.(\d{1,2})\.(\d{4})\s*$")]
    private static partial Regex DottedDateRegex();

    [GeneratedRegex(@"^\s*(?<startDay>\d{1,2})\.-(?<endDay>\d{1,2})\.(?<month>\d{1,2})\.(?<year>\d{4})\s*$")]
    private static partial Regex DuvDateRangeNoMonthRegex();

    [GeneratedRegex(@"^\s*(?<startDay>\d{1,2})\.(?<startMonth>\d{1,2})\.-(?<endDay>\d{1,2})\.(?<endMonth>\d{1,2})\.(?<year>\d{4})\s*$")]
    private static partial Regex DuvDateRangeMonthRegex();

    [GeneratedRegex(@"^\s*(?<startDay>\d{1,2})\.(?<startMonth>\d{1,2})\.(?<year>\d{4})-(?<endDay>\d{1,2})\.(?<endMonth>\d{1,2})\.(?<endYear>\d{4})\s*$")]
    private static partial Regex DuvDateRangeFullRegex();





    [GeneratedRegex(@"(\d+)(?:st|nd|rd|th)\b", RegexOptions.IgnoreCase)]
    private static partial Regex OrdinalSuffixRegex();

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

    /// <summary>
    /// Derives the event key for a ScrapeJob by picking the best available URL. Normalizes organizer ids.
    /// Priority: WebsiteUrl → UtmbUrl → TraceDeTrailEventUrl → RunagainUrl → BetrailUrl → name-based fallback.
    /// </summary>
    public static (string EventKey, string CanonicalUrl)? DeriveEventKeyFromJob(ScrapeJob job)
    {
        Uri? bestUrl = job.WebsiteUrl
            ?? job.UtmbUrl
            ?? job.TraceDeTrailEventUrl
            ?? job.RunagainUrl
            ?? job.BetrailUrl;
        if (bestUrl is not null)
            return (RaceOrganizerClient.DeriveOrganizerKey(bestUrl), bestUrl.AbsoluteUri);

        // No URL at all — derive from name (last resort).
        var fallbackKey = BuildFeatureId(job.Name, job.Distance);
        if (!string.IsNullOrEmpty(fallbackKey))
            return (fallbackKey, $"name://{fallbackKey}");

        return null;
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
        if (!Uri.TryCreate(raw.Trim(), UriKind.Absolute, out var uri)) return null;
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


    /// <inheritdoc cref="RaceDistanceKm.TryParseMarathonKeyword" />
    public static bool TryParseMarathonKeyword(string token, out double km) =>
        RaceDistanceKm.TryParseMarathonKeyword(token, out km);

    // Parses a verbose distance string into a list of (km, formatted) pairs.
    // Marathon keywords are translated; non-parseable tokens are skipped.
    private static IReadOnlyList<(double Km, string Formatted)> ParseVerboseDistanceParts(string distanceVerbose)
    {
        var parts = distanceVerbose
            .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        var result = new List<(double, string)>(parts.Length);
        foreach (var part in parts)
        {
            if (RaceDistanceKm.TryParseCommaListTokenKilometers(part, out var km))
                result.Add((km, FormatDistanceKm(km)));
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
                if (RaceDistanceKm.WithinRelativeOfReference(verboseKm, routeDistancesKm[i], 0.25) && delta < bestDelta)
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
    IReadOnlyDictionary<string, string>? ExternalIds = null, // issuer/authority → opaque ID (e.g. "utmb" → "133", "tracedetrail" → "12345")
    string? Distance = null,     // pre-formatted, e.g. "50 km" or "50 km, 25 km"
    double? ElevationGain = null,
    string? Country = null,
    string? Location = null,
    string? RaceType = null,
    bool? RegistrationOpen = null,
    string? Date = null,
    string? ImageUrl = null,
    string? LogoUrl = null,
    double? Latitude = null,
    double? Longitude = null,
    IReadOnlyList<string>? Playgrounds = null,
    int? ItraPoints = null,
    int? RunningStones = null,
    string? UtmbWorldSeriesCategory = null,
    string? County = null,
    string? TypeLocal = null,
    string? Organizer = null,
    string? Description = null,
    string? StartFee = null,
    string? Currency = null,
    // Per-source URLs (null = source not available for this race).
    Uri? UtmbUrl = null,
    IReadOnlyList<Uri>? TraceDeTrailItraUrls = null,
    Uri? TraceDeTrailEventUrl = null,
    Uri? ItraEventPageUrl = null,
    bool? ItraNationalLeague = null,
    Uri? RunagainUrl = null,
    Uri? WebsiteUrl = null,         // generic race website (e.g. from Loppkartan)
    Uri? BetrailUrl = null)
{
    /// <summary>
    /// Converts this ScrapeJob to a <see cref="SourceDiscovery"/> for storage in a working document.
    /// </summary>
    public Shared.Models.SourceDiscovery ToSourceDiscovery() => new()
    {
        DiscoveredAtUtc = DateTime.UtcNow.ToString("o"),
        Name = Name,
        Date = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(Date) ?? Date,
        Latitude = Latitude,
        Longitude = Longitude,
        Distance = Distance,
        ElevationGain = ElevationGain,
        Country = RaceScrapeDiscovery.NormalizeCountryToIso2(Country) ?? Country,
        Location = Location,
        RaceType = RaceScrapeDiscovery.NormalizeRaceType(RaceType) ?? RaceType,
        ImageUrl = ImageUrl,
        LogoUrl = LogoUrl,
        Organizer = Organizer,
        Description = Description,
        StartFee = StartFee,
        Currency = Currency,
        County = County,
        TypeLocal = TypeLocal,
        RegistrationOpen = RegistrationOpen,
        ExternalIds = ExternalIds is { Count: > 0 } ? new Dictionary<string, string>(ExternalIds) : null,
        SourceUrls = GetSourceUrls(),
        ItraPoints = ItraPoints,
        ItraNationalLeague = ItraNationalLeague,
        Playgrounds = Playgrounds is { Count: > 0 } ? [.. Playgrounds] : null,
        RunningStones = RunningStones,
        UtmbWorldSeriesCategory = UtmbWorldSeriesCategory,
    };

    private List<string>? GetSourceUrls()
    {
        var urls = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void Add(Uri? uri)
        {
            if (uri is null) return;
            var absolute = uri.AbsoluteUri;
            if (seen.Add(absolute))
                urls.Add(absolute);
        }

        Add(UtmbUrl);
        if (TraceDeTrailItraUrls is { Count: > 0 })
        {
            foreach (var u in TraceDeTrailItraUrls)
                Add(u);
        }
        Add(TraceDeTrailEventUrl);
        Add(ItraEventPageUrl);
        Add(RunagainUrl);
        Add(WebsiteUrl);
        Add(BetrailUrl);

        return urls.Count > 0 ? urls : null;
    }
}

public record TraceDeTrailTraceData(
    IReadOnlyList<(double Lng, double Lat)> Points,
    double? TotalDistanceKm,
    double? ElevationGain);

public record TraceDeTrailCourseInfo(
    string? Name,
    string? Distance,
    double? ElevationGain,
    int? ItraPoints,
    int? TraceId);
