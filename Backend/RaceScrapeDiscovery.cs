using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using Shared.Services;

namespace Backend;

public static partial class RaceScrapeDiscovery
{
    private static readonly char[] DistanceListSeparators = [',', ';'];

    // Hosts whose discovered events should be silently ignored (test/placeholder domains).
    private static readonly HashSet<string> IgnoredHosts = new(StringComparer.OrdinalIgnoreCase)
    {
        "test.com"
    };

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

        // Strip a leading time like "12:00, " that may precede the date.
        trimmed = LeadingTimeRegex().Replace(trimmed, "").Trim();

        // Strip ordinal suffixes (1st, 2nd, 3rd, 4th, etc.) before parsing
        var cleaned = OrdinalSuffixRegex().Replace(trimmed, "$1");

        // Replace non-English month names with English equivalents for InvariantCulture parsing.
        cleaned = ReplaceLocalizedMonths(cleaned);
        // Strip Swedish/English time markers like "kl. 10:00" that appear after dates.
        cleaned = TimeSuffixRegex().Replace(cleaned, "");
        // Remove weekday prefixes (Swedish, Norwegian, English).
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

        // Yearless formats like "September 11" or "11 September" — infer nearest upcoming year.
        cleaned = cleaned.Trim().Trim(',').Trim();
        foreach (var fmt in new[] { "MMMM d", "d MMMM", "MMM d", "d MMM" })
        {
            if (DateOnly.TryParseExact(cleaned, fmt, CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var yearless))
            {
                var today = DateOnly.FromDateTime(DateTime.UtcNow);
                var candidate = new DateOnly(today.Year, yearless.Month, yearless.Day);
                if (candidate < today)
                    candidate = candidate.AddYears(1);
                return candidate.ToString("yyyy-MM-dd");
            }
        }

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

    [GeneratedRegex(@"\b(?:måndag|tisdag|onsdag|torsdag|fredag|lördag|söndag|mandag|tirsdag|onsdag|torsdag|fredag|lordag|sondag|monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", RegexOptions.IgnoreCase)]
    private static partial Regex WeekdayRegex();

    // Matches a leading time like "12:00, " or "12:00 - " before a date.
    [GeneratedRegex(@"^\d{1,2}:\d{2}\s*[,\-–]?\s*", RegexOptions.None)]
    private static partial Regex LeadingTimeRegex();

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
    public static string? MatchDistanceKmToVerbose(double distanceKm, string? distanceVerbose, double tolerance = 0.25)
    {
        if (string.IsNullOrWhiteSpace(distanceVerbose) || distanceKm <= 0)
            return null;

        var parts = distanceVerbose
            .Split(DistanceListSeparators, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

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

        return RaceDistanceKm.WithinRelativeOfReference(distanceKm, bestMatchedKm.Value, tolerance)
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
            .Split(DistanceListSeparators, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

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
        if (bestUrl is not null && IsIgnoredHost(bestUrl))
            return null;
        if (bestUrl is not null)
            return (RaceOrganizerClient.DeriveOrganizerKey(bestUrl), bestUrl.AbsoluteUri);

        // No URL at all — derive from name (last resort).
        var fallbackKey = BuildFeatureId(job.Name, job.Distance);
        if (!string.IsNullOrEmpty(fallbackKey))
            return (fallbackKey, $"name://{fallbackKey}");

        return null;
    }

    private static bool IsIgnoredHost(Uri url)
    {
        var host = url.Host;
        if (host.StartsWith("www.", StringComparison.OrdinalIgnoreCase))
            host = host[4..];
        return IgnoredHosts.Contains(host);
    }

    // Matches one or more characters that are not Unicode letters, digits, or hyphens.
    // Used to sanitise names/distances into URL slug form.
    [GeneratedRegex(@"[^\p{L}\p{N}]+")]
    private static partial Regex NonSlugCharsRegex();
}
