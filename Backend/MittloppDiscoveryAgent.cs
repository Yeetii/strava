using System.Globalization;
using System.Net;
using System.Text.RegularExpressions;
using Shared.Services;

namespace Backend;

public static partial class MittloppDiscoveryAgent
{
    public sealed record RegistrationVariantCandidate(Uri Url, string Label);

    public static readonly Uri CalendarUrl = new("https://mittlopp.se/kalender");

    private static readonly HashSet<string> SupportedRaceTypes =
    [
        "running",
        "cycling",
        "triathlon",
        "swimrun",
        "obstacle course"
    ];

    public static IReadOnlyCollection<ScrapeJob> ParseCalendarPage(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var jobs = new List<ScrapeJob>();
        foreach (var block in ExtractHtmlElementsByClass(html, "div", "calendar-item"))
        {
            var href = ExtractAnchorHref(block);
            var eventUrl = ResolveUri(href, pageUrl);
            if (eventUrl is null)
                continue;

            var name = ExtractElementText(block, "h3");
            if (string.IsNullOrWhiteSpace(name))
                continue;

            var imageUrl = ExtractAttributeValue(block, "src");
            var dateText = ExtractTextByClass(block, "race-date-container");
            var location = ExtractTextByClass(block, "race-place-container");
            var summary = ExtractTextByClass(block, "race-distance-container");
            var badgeTitle = ExtractAttributeValueByClass(block, "race-type-badge", "title");
            var iconAlts = ExtractImageAltTextsByClass(block, "tri-icon-image");

            var mittloppUrl = IsMittloppEventUrl(eventUrl) ? eventUrl : null;
            var websiteUrl = mittloppUrl is null ? eventUrl : eventUrl;
            var date = NormalizeMittloppDate(dateText) ?? RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(dateText);

            var externalIds = mittloppUrl is null
                ? null
                : new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["mittlopp"] = BuildMittloppId(mittloppUrl)
                };

            var typeTokens = new[]
            {
                name,
                summary,
                badgeTitle,
                string.Join(", ", iconAlts)
            };

            jobs.Add(new ScrapeJob(
                WebsiteUrl: websiteUrl,
                MittloppEventUrl: mittloppUrl,
                Name: name,
                ExternalIds: externalIds,
                Date: date,
                Country: "SE",
                Location: location,
                Distance: NormalizeDistance(summary),
                ImageUrl: imageUrl,
                RaceType: InferRaceType(typeTokens),
                TypeLocal: InferTypeLocal(typeTokens)));
        }

        return jobs;
    }

    public static Uri? ExtractNextPageUrl(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return null;

        var match = Regex.Match(html,
            "<a[^>]*class=['\"][^'\"]*btn-page-next[^'\"]*['\"][^>]*href=['\"](?<href>[^'\"]+)['\"]",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success)
            return null;

        return ResolveUri(match.Groups["href"].Value, pageUrl);
    }

    public static IReadOnlyCollection<RegistrationVariantCandidate> ExtractRegistrationVariants(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return [];

        var variants = new List<RegistrationVariantCandidate>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (Match match in Regex.Matches(html,
                     "<a[^>]*id=['\"][^'\"]*subCompRepeater_link_[^'\"]*['\"][^>]*href=['\"](?<href>[^'\"]+)['\"][^>]*>(?<content>.*?)</a>",
                     RegexOptions.IgnoreCase | RegexOptions.Singleline))
        {
            var url = ResolveUri(WebUtility.HtmlDecode(match.Groups["href"].Value), pageUrl);
            if (url is null)
                continue;

            if (!seen.Add(url.AbsoluteUri))
                continue;

            var label = NormalizeWhitespace(HtmlText.DecodeAndStripTags(match.Groups["content"].Value));
            variants.Add(new RegistrationVariantCandidate(url, label));
        }

        return variants;
    }

    public static Uri? SelectBestRegistrationVariantUrl(ScrapeJob seedJob, IReadOnlyCollection<RegistrationVariantCandidate> variants)
    {
        if (variants.Count == 0)
            return null;

        var ranked = variants
            .Select(variant => (variant.Url, Score: ScoreRegistrationVariant(seedJob, variant)))
            .OrderByDescending(x => x.Score)
            .ThenBy(x => x.Url.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return ranked[0].Url;
    }

    public static ScrapeJob EnrichFromEventHubPage(ScrapeJob job, string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return job;

        var name = ExtractElementText(html, "h1");
        var location = ExtractLabelRowValue(html, "Ort/plats:") ?? ExtractLabelRowValue(html, "Plats:");
        var homepageUrl = ExtractLabeledAnchorUrl(html, "Hemsida:", pageUrl);
        var organizer = ExtractLabelRowValue(html, "Arrangör:");
        var description = ExtractFormControlStaticText(html);
        var imageUrl = ResolveUri(ExtractAttributeValueById(html, "MainSection_headerImage", "src"), pageUrl)?.AbsoluteUri;

        var updated = job with
        {
            Name = name ?? job.Name,
            Location = location ?? job.Location,
            Organizer = organizer ?? job.Organizer,
            Description = description ?? job.Description,
            ImageUrl = imageUrl ?? job.ImageUrl,
            WebsiteUrl = homepageUrl ?? job.WebsiteUrl,
            MittloppEventUrl = job.MittloppEventUrl ?? pageUrl,
            ExternalIds = EnsureMittloppExternalId(job.ExternalIds, job.MittloppEventUrl ?? pageUrl)
        };

        updated = updated with
        {
            RaceType = updated.RaceType ?? InferRaceType(updated.Name, updated.Description),
            TypeLocal = updated.TypeLocal ?? InferTypeLocal(updated.Name, updated.Description)
        };

        return updated;
    }

    public static ScrapeJob EnrichFromRegistrationVariantPage(ScrapeJob job, string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
            return job;

        var triathlonDistance = ExtractLabelRowValue(html, "Triatlondistans:");
        var sport = ExtractLabelRowValue(html, "Sport:");
        var distance = ExtractLabelRowValue(html, "Distans:");
        var swim = ExtractLabelRowValue(html, "Simning:");
        var bike = ExtractLabelRowValue(html, "Cykel:");
        var run = ExtractLabelRowValue(html, "Löpning:");
        var startTime = ExtractLabelRowValue(html, "Starttid:");
        var date = ExtractLabelRowValue(html, "Datum:");
        var location = ExtractLabelRowValue(html, "Plats:");
        var homepageUrl = ExtractLabeledAnchorUrl(html, "Hemsida:", pageUrl);
        var organizer = ExtractLabelRowValue(html, "Arrangör:");
        var description = ExtractFormControlStaticText(html);
        var startFee = ExtractLowestPrice(html);
        var deadline = ExtractLabelRowValue(html, "Sista anmälan:");
        var imageUrl = ResolveUri(ExtractAttributeValueById(html, "MainSection_headerImage", "src"), pageUrl)?.AbsoluteUri;
        var name = ExtractElementText(html, "h1");

        var builtDistance = BuildTriathlonDistance(swim, bike, run)
            ?? NormalizeDistance(distance)
            ?? NormalizeDistance(triathlonDistance)
            ?? job.Distance;

        var descriptionWithDeadline = AppendDescriptionNotes(description ?? job.Description, deadline);
        var combinedTokens = new[]
        {
            name,
            sport,
            triathlonDistance,
            distance,
            swim,
            bike,
            run,
            descriptionWithDeadline,
            job.Name,
            job.TypeLocal
        };

        return job with
        {
            Name = name ?? job.Name,
            Date = NormalizeMittloppDate(startTime) ?? NormalizeMittloppDate(date) ?? job.Date,
            Location = location ?? job.Location,
            Distance = builtDistance,
            RaceType = InferRaceType(combinedTokens),
            TypeLocal = FirstNonBlank(triathlonDistance, sport, distance, InferTypeLocal(combinedTokens), job.TypeLocal),
            Organizer = organizer ?? job.Organizer,
            Description = descriptionWithDeadline,
            StartFee = startFee ?? job.StartFee,
            Currency = startFee is not null ? "SEK" : job.Currency,
            WebsiteUrl = homepageUrl ?? job.WebsiteUrl,
            MittloppEventUrl = pageUrl,
            ExternalIds = EnsureMittloppExternalId(job.ExternalIds, pageUrl),
            ImageUrl = imageUrl ?? job.ImageUrl
        };
    }

    public static string? InferRaceType(params string?[] values)
    {
        var haystack = BuildSearchHaystack(values);
        if (string.IsNullOrWhiteSpace(haystack))
            return null;

        if (ContainsAny(haystack, ["duathlon"]))
            return null;

        if (ContainsAny(haystack, ["hinderbana", "ocr", "obstacle course"]))
            return "obstacle course";

        if (ContainsAny(haystack, ["swimrun"]))
            return "swimrun";

        if (ContainsAny(haystack, ["triathlon", "triatlon", "supersprint", "olympisk", "medeldistans", "långdistans", "trytri", "tri4fun", "terrängtriathlon"]))
            return "triathlon";

        if (ContainsAny(haystack, ["cykel", "gravel", "mtb", "mountainbike", "bike"]))
            return "cycling";

        if (ContainsAny(haystack, ["promenad", "simskola"]))
            return null;

        if (ContainsAny(haystack, ["löpning", "lopning", "run", "trail", "marathon", "backyard", "lopp", "ultra", "km", "mil"]))
            return "running";

        return null;
    }

    public static bool ShouldKeepJob(ScrapeJob job)
        => !string.IsNullOrWhiteSpace(job.RaceType)
           && SupportedRaceTypes.Contains(job.RaceType);

    public static string? NormalizeMittloppDate(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        var normalized = HtmlText.DecodeAndStripTags(value)
            .Replace("\u00a0", " ", StringComparison.Ordinal)
            .Trim();

        var startOnlyMatch = Regex.Match(normalized, @"(?<date>\d{4}-\d{2}-\d{2})\s*[–-]\s*(?:\d{4}-\d{2}-\d{2}|\d{2}(?:-\d{2})?)");
        if (startOnlyMatch.Success)
            return startOnlyMatch.Groups["date"].Value;

        var embeddedIso = Regex.Match(normalized, @"\b\d{4}-\d{2}-\d{2}\b");
        if (embeddedIso.Success)
            return embeddedIso.Value;

        return RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(normalized);
    }

    private static IReadOnlyDictionary<string, string>? EnsureMittloppExternalId(IReadOnlyDictionary<string, string>? existing, Uri eventUrl)
    {
        var id = BuildMittloppId(eventUrl);
        if (existing is not null && existing.TryGetValue("mittlopp", out _))
            return existing;

        var updated = existing is null
            ? new Dictionary<string, string>(StringComparer.Ordinal)
            : new Dictionary<string, string>(existing, StringComparer.Ordinal);
        updated["mittlopp"] = id;
        return updated;
    }

    private static string BuildMittloppId(Uri eventUrl)
    {
        var path = eventUrl.AbsolutePath.Trim('/');
        return string.IsNullOrWhiteSpace(path) ? eventUrl.AbsoluteUri : path.ToLowerInvariant();
    }

    private static string? BuildTriathlonDistance(string? swim, string? bike, string? run)
    {
        var parts = new List<string>();
        if (!string.IsNullOrWhiteSpace(swim))
            parts.Add($"{NormalizeWhitespace(swim)} swim");
        if (!string.IsNullOrWhiteSpace(bike))
            parts.Add($"{NormalizeWhitespace(bike)} bike");
        if (!string.IsNullOrWhiteSpace(run))
            parts.Add($"{NormalizeWhitespace(run)} run");
        return parts.Count > 0 ? string.Join(", ", parts) : null;
    }

    private static string? ExtractLowestPrice(string html)
    {
        var prices = new List<decimal>();
        foreach (Match match in Regex.Matches(html, @">\s*(?<value>\d+[\d\s]*)\s*kr\s*<", RegexOptions.IgnoreCase | RegexOptions.Singleline))
        {
            var digits = Regex.Replace(match.Groups["value"].Value, "[^0-9]", string.Empty);
            if (decimal.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed))
                prices.Add(parsed);
        }

        if (prices.Count == 0)
            return null;

        var min = prices.Min();
        return string.Create(CultureInfo.InvariantCulture, $"{min:0} kr");
    }

    private static string? AppendDescriptionNotes(string? description, string? deadline)
    {
        var cleanedDescription = NormalizeWhitespace(description);
        if (string.IsNullOrWhiteSpace(deadline))
            return cleanedDescription;

        return string.IsNullOrWhiteSpace(cleanedDescription)
            ? $"Registration deadline: {NormalizeWhitespace(deadline)}"
            : $"{cleanedDescription}\n\nRegistration deadline: {NormalizeWhitespace(deadline)}";
    }

    private static string? InferTypeLocal(params string?[] values)
    {
        var haystack = BuildSearchHaystack(values);
        if (string.IsNullOrWhiteSpace(haystack))
            return null;

        if (ContainsAny(haystack, ["hinderbana"]))
            return "Hinderbana";
        if (ContainsAny(haystack, ["swimrun"]))
            return "Swimrun";
        if (ContainsAny(haystack, ["terrängtriathlon"]))
            return "Terrängtriathlon";
        if (ContainsAny(haystack, ["olympisk"]))
            return "Olympisk";
        if (ContainsAny(haystack, ["sprint"]))
            return "Sprint";
        if (ContainsAny(haystack, ["medeldistans"]))
            return "Medeldistans";
        if (ContainsAny(haystack, ["långdistans"]))
            return "Långdistans";
        if (ContainsAny(haystack, ["motion"]))
            return "Motion";
        return null;
    }

    private static int ScoreRegistrationVariant(ScrapeJob seedJob, RegistrationVariantCandidate variant)
    {
        var score = 0;
        var path = variant.Url.AbsolutePath.ToLowerInvariant();
        var label = variant.Label.ToLowerInvariant();
        var seedName = (seedJob.Name ?? string.Empty).ToLowerInvariant();
        var seedDistance = (seedJob.Distance ?? string.Empty).ToLowerInvariant();
        var primaryToken = ExtractPrimaryVariantToken(seedJob);

        if (path.Contains("tri4fun", StringComparison.Ordinal)
            || label.Contains("tri4fun", StringComparison.Ordinal)
            || path.Contains("promenad", StringComparison.Ordinal)
            || label.Contains("promenad", StringComparison.Ordinal)
            || path.Contains("simskola", StringComparison.Ordinal))
        {
            score -= 100;
        }

        if (path.Contains("lag", StringComparison.Ordinal) || label.Contains(" lag ", StringComparison.Ordinal))
            score -= 15;
        if (label.Contains("barn", StringComparison.Ordinal) || label.Contains("mini", StringComparison.Ordinal))
            score -= 25;

        if (seedName.Contains("halvmarathon", StringComparison.Ordinal) && path.Contains("halvmarathon", StringComparison.Ordinal))
            score += 50;
        if (seedName.Contains("halvmarathon", StringComparison.Ordinal) && label.Contains("halvmarathon", StringComparison.Ordinal))
            score += 50;
        if (seedName.Contains("swimrun", StringComparison.Ordinal) && path.Contains("swimrun", StringComparison.Ordinal))
            score += 50;
        if (seedName.Contains("hinderbana", StringComparison.Ordinal) && path.Contains("hinderbana", StringComparison.Ordinal))
            score += 50;

        if (!string.IsNullOrWhiteSpace(primaryToken))
        {
            var normalizedPrimary = primaryToken.ToLowerInvariant();
            if (label.Contains(normalizedPrimary, StringComparison.Ordinal))
                score += 80;
            if (path.Contains(normalizedPrimary.Replace(" ", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal))
                score += 60;

            if (normalizedPrimary.Contains("halvmarathon", StringComparison.Ordinal) && (path.Contains("halvmarathon", StringComparison.Ordinal) || label.Contains("halvmarathon", StringComparison.Ordinal)))
                score += 80;
            if (normalizedPrimary.Contains("10 km", StringComparison.Ordinal) && (path.Contains("10km", StringComparison.Ordinal) || label.Contains("10 km", StringComparison.Ordinal)))
                score += 80;
            if (normalizedPrimary.Contains("5 km", StringComparison.Ordinal) && (path.Contains("5km", StringComparison.Ordinal) || label.Contains("5 km", StringComparison.Ordinal)))
                score += 80;
            if ((normalizedPrimary.Contains("0.2 km", StringComparison.Ordinal) || normalizedPrimary.Contains("0,2 km", StringComparison.Ordinal)) && (path.Contains("mini", StringComparison.Ordinal) || label.Contains("200 m", StringComparison.Ordinal)))
                score += 80;
            if ((normalizedPrimary.Contains("0.6 km", StringComparison.Ordinal) || normalizedPrimary.Contains("0,6 km", StringComparison.Ordinal)) && (path.Contains("yngre-barn", StringComparison.Ordinal) || label.Contains("600 m", StringComparison.Ordinal)))
                score += 80;
            if ((normalizedPrimary.Contains("1.5 km", StringComparison.Ordinal) || normalizedPrimary.Contains("1,5 km", StringComparison.Ordinal)) && label.Contains("1,5 km", StringComparison.Ordinal))
                score += 70;
        }

        if (seedJob.RaceType == "triathlon" && !path.Contains("tri4fun", StringComparison.Ordinal))
            score += 10;

        return score;
    }

    private static string? ExtractPrimaryVariantToken(ScrapeJob seedJob)
    {
        if (!string.IsNullOrWhiteSpace(seedJob.Distance))
        {
            var firstDistanceToken = seedJob.Distance
                .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                .FirstOrDefault();
            if (!string.IsNullOrWhiteSpace(firstDistanceToken))
                return NormalizeWhitespace(firstDistanceToken);
        }

        if (!string.IsNullOrWhiteSpace(seedJob.TypeLocal))
            return NormalizeWhitespace(seedJob.TypeLocal);

        return null;
    }

    private static string? NormalizeDistance(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return RaceScrapeDiscovery.ParseDistanceVerbose(value) ?? NormalizeWhitespace(value);
    }

    private static bool IsMittloppEventUrl(Uri url)
        => url.Host.Contains("mittlopp.se", StringComparison.OrdinalIgnoreCase)
           && url.AbsolutePath.Contains("/anm/", StringComparison.OrdinalIgnoreCase);

    private static string BuildSearchHaystack(IEnumerable<string?> values)
        => string.Join(" ", values.Where(v => !string.IsNullOrWhiteSpace(v)).Select(NormalizeWhitespace)).ToLowerInvariant();

    private static bool ContainsAny(string haystack, IEnumerable<string> needles)
        => needles.Any(needle => haystack.Contains(needle, StringComparison.OrdinalIgnoreCase));

    private static string? FirstNonBlank(params string?[] values)
        => values.FirstOrDefault(v => !string.IsNullOrWhiteSpace(v)) is string value ? NormalizeWhitespace(value) : null;

    private static string? ExtractAnchorHref(string html)
    {
        var match = Regex.Match(html, "<a[^>]*href=['\"](?<href>[^'\"]+)['\"]", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? WebUtility.HtmlDecode(match.Groups["href"].Value.Trim()) : null;
    }

    private static string? ExtractLabelRowValue(string html, string label)
    {
        var pattern = $@"<div[^>]*class=['""'][^'""']*labelrow[^'""']*['""'][^>]*>.*?<(?:span|label)[^>]*>(?:{Regex.Escape(label)})</(?:span|label)>.*?<div[^>]*class=['""'][^'""']*col-xs-8[^'""']*['""'][^>]*>(?<value>.*?)</div>";
        var match = Regex.Match(html, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? NormalizeWhitespace(HtmlText.DecodeAndStripTags(match.Groups["value"].Value)) : null;
    }

    private static Uri? ExtractLabeledAnchorUrl(string html, string label, Uri pageUrl)
    {
        var pattern = $@"<div[^>]*class=['""'][^'""']*labelrow[^'""']*['""'][^>]*>.*?<(?:span|label)[^>]*>(?:{Regex.Escape(label)})</(?:span|label)>.*?<a[^>]*href=['""'](?<href>[^'""']+)['""']";
        var match = Regex.Match(html, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? ResolveUri(WebUtility.HtmlDecode(match.Groups["href"].Value.Trim()), pageUrl) : null;
    }

    private static string? ExtractFormControlStaticText(string html)
    {
        var match = Regex.Match(html, "<p[^>]*class=['\"][^'\"]*form-control-static[^'\"]*['\"][^>]*>(?<value>.*?)</p>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!match.Success)
            return null;

        var normalized = Regex.Replace(match.Groups["value"].Value, @"<br\s*/?>", "\n", RegexOptions.IgnoreCase);
        var decoded = HtmlText.DecodeAndStripTags(normalized)
            .Replace("\r", string.Empty, StringComparison.Ordinal)
            .Trim();
        return Regex.Replace(decoded, @"[^\S\n]+", " ").Trim();
    }

    private static string? ExtractAttributeValueById(string html, string id, string attributeName)
    {
        var pattern = $@"<[^>]*id=['""']{Regex.Escape(id)}['""'][^>]*{Regex.Escape(attributeName)}=['""'](?<value>[^'""']+)['""']";
        var match = Regex.Match(html, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? WebUtility.HtmlDecode(match.Groups["value"].Value.Trim()) : null;
    }

    private static string? ExtractAttributeValueByClass(string html, string className, string attributeName)
    {
        var pattern = $@"<[^>]*class=['""'][^'""']*{Regex.Escape(className)}[^'""']*['""'][^>]*{Regex.Escape(attributeName)}=['""'](?<value>[^'""']+)['""']";
        var match = Regex.Match(html, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? WebUtility.HtmlDecode(match.Groups["value"].Value.Trim()) : null;
    }

    private static IReadOnlyCollection<string> ExtractImageAltTextsByClass(string html, string className)
    {
        var values = new List<string>();
        var pattern = $@"<img[^>]*class=['""'][^'""']*{Regex.Escape(className)}[^'""']*['""'][^>]*alt=['""'](?<value>[^'""']+)['""']";
        foreach (Match match in Regex.Matches(html, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline))
        {
            var value = NormalizeWhitespace(match.Groups["value"].Value);
            if (!string.IsNullOrWhiteSpace(value))
                values.Add(value);
        }

        return values;
    }

    private static string? ExtractTextByClass(string html, string className)
    {
        var pattern = $@"<div[^>]*class=['""'][^'""']*{Regex.Escape(className)}[^'""']*['""'][^>]*>(?<value>.*?)</div>";
        var match = Regex.Match(html, pattern, RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? NormalizeWhitespace(HtmlText.DecodeAndStripTags(match.Groups["value"].Value)) : null;
    }

    private static string? ExtractElementText(string html, string elementName)
    {
        var match = Regex.Match(html, $"<\\s*{elementName}[^>]*>(?<value>.*?)</\\s*{elementName}\\s*>", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? NormalizeWhitespace(HtmlText.DecodeAndStripTags(match.Groups["value"].Value)) : null;
    }

    private static string? ExtractAttributeValue(string html, string attributeName)
    {
        var match = Regex.Match(html, $@"{Regex.Escape(attributeName)}\s*=\s*(?:(['""'])(?<value>.*?)\1|(?<value>[^\s>]+))", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        return match.Success ? WebUtility.HtmlDecode(match.Groups["value"].Value.Trim()) : null;
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

        return Uri.TryCreate(baseUrl, value, out var relativeUri)
            ? relativeUri
            : null;
    }

    private static IReadOnlyCollection<string> ExtractHtmlElementsByClass(string html, string tagName, string className)
    {
        var results = new List<string>();
        var pattern = $"<\\s*{tagName}\\b[^>]*\\bclass\\s*=\\s*['\"]([^'\"]*\\b{Regex.Escape(className)}\\b[^'\"]*)['\"][^>]*>";
        foreach (Match match in Regex.Matches(html, pattern, RegexOptions.IgnoreCase))
        {
            if (TryExtractHtmlElement(html, match.Index, tagName, out var element))
                results.Add(element);
        }

        return results;
    }

    private static bool TryExtractHtmlElement(string html, int startIndex, string tagName, out string element)
    {
        element = string.Empty;
        var lowerHtml = html.ToLowerInvariant();
        var lowerTag = tagName.ToLowerInvariant();
        var depth = 0;
        var index = startIndex;
        while (index < lowerHtml.Length)
        {
            var next = lowerHtml.IndexOf('<', index);
            if (next == -1)
                break;

            if (IsOpeningTagAt(lowerHtml, next, lowerTag))
            {
                depth++;
            }
            else if (IsClosingTagAt(lowerHtml, next, lowerTag))
            {
                depth--;
                if (depth == 0)
                {
                    var closeEnd = html.IndexOf('>', next);
                    if (closeEnd == -1)
                        closeEnd = html.Length - 1;

                    element = html.Substring(startIndex, closeEnd - startIndex + 1);
                    return true;
                }
            }

            index = next + 1;
        }

        return false;
    }

    private static bool IsOpeningTagAt(string html, int index, string tagName)
    {
        if (index + 1 >= html.Length || html[index + 1] == '/')
            return false;

        var span = html.AsSpan(index + 1);
        if (!span.StartsWith(tagName, StringComparison.Ordinal))
            return false;

        if (span.Length == tagName.Length)
            return false;

        var nextChar = span[tagName.Length];
        return nextChar == ' ' || nextChar == '\t' || nextChar == '\r' || nextChar == '\n' || nextChar == '>';
    }

    private static bool IsClosingTagAt(string html, int index, string tagName)
    {
        var span = html.AsSpan(index + 1);
        return span.StartsWith('/' + tagName, StringComparison.Ordinal);
    }

    private static string NormalizeWhitespace(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        var normalized = WebUtility.HtmlDecode(text)
            .Replace("\u00a0", " ", StringComparison.Ordinal)
            .Replace("\r", " ", StringComparison.Ordinal)
            .Replace("\n", " ", StringComparison.Ordinal);
        return Regex.Replace(normalized, "\\s+", " ").Trim();
    }
}
