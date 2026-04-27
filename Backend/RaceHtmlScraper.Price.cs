using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Backend.Scrapers;
using Shared.Services;

namespace Backend;

public static partial class RaceHtmlScraper
{
    // Matches elevation gain keywords followed by a number, or a number followed by elevation keywords.
    // Patterns: "elevation gain: 1200 m", "Elevation Gain ↙ 900 m ↘ 1050 m", "höjdmeter: 1200", "D+ 1200m".
    // Allows up to 40 chars gap between keyword and number (tags are stripped first).
    private static readonly string[] ElevationKeywords =
        ["elevation gain", "ascent", "dénivelé positif", "dénivelé", "denivelé", "denivele",
         "aufstieg", "höjdmeter", "höhenmeter", "hohenmeter", "stigning", "totalstigning", "d+"];

    [GeneratedRegex(@"\b([+-]?(?:\d[\d\s]{0,6}\d|\d{1,5}))(?:\s*(?:m\b|meter\b|hm\b))?", RegexOptions.IgnoreCase)]
    private static partial Regex ElevationNumberRegex();

    /// <summary>
    /// Extracts elevation gain (in metres) from visible text in an HTML page.
    /// Finds the largest plausible value (1–99999 m) near an elevation keyword.
    /// </summary>
    public static double? ExtractElevationGain(string? html)
    {
        if (string.IsNullOrWhiteSpace(html)) return null;

        var text = HtmlTagRegex().Replace(html, " ");

        var pageDistanceKm = ExtractPageDistanceKm(text);
        double? best = null;
        foreach (var keyword in ElevationKeywords)
        {
            var kwIdx = text.IndexOf(keyword, StringComparison.OrdinalIgnoreCase);
            if (kwIdx < 0) continue;

            var lineStart = text.LastIndexOfAny(['\r', '\n'], kwIdx);
            lineStart = lineStart < 0 ? 0 : lineStart + 1;
            var lineEnd = text.IndexOfAny(['\r', '\n'], kwIdx + keyword.Length);
            if (lineEnd < 0) lineEnd = text.Length;
            var line = text[lineStart..lineEnd];

            foreach (Match match in ElevationNumberRegex().Matches(line))
            {
                var raw = match.Groups[1].Value.Replace(" ", "");
                if (double.TryParse(raw, NumberStyles.Float,
                    CultureInfo.InvariantCulture, out var metres)
                    && metres >= 1 && metres <= 99999)
                {
                    if (pageDistanceKm is not null && metres > pageDistanceKm.Value * 500)
                        continue;

                    // Take the largest value on the same line as the keyword.
                    if (!best.HasValue || metres > best.Value)
                        best = metres;
                }
            }
        }
        return best;
    }

    /// <summary>
    /// Extracts a price from visible HTML text.
    /// Looks for patterns like "500 kr", "€50", "SEK 300", "$120", "300 NOK", "1 200 SEK".
    /// Returns (amount, currency) or null.
    /// </summary>
    public static PriceInfo? ExtractPrice(string? html, Uri? pageUrl = null)
    {
        if (string.IsNullOrWhiteSpace(html)) return null;

        var tld = pageUrl?.Host.Split('.').LastOrDefault()?.ToLowerInvariant();

        // Strip non-visible content.
        var clean = HeadSectionRegex().Replace(html, " ");
        clean = ScriptStyleRegex().Replace(clean, " ");
        var text = HtmlTagRegex().Replace(clean, " ");

        // 1. JSON-LD "offers" → "price" + "priceCurrency".
        var priceMatch = JsonLdPriceRegex().Match(html);
        if (priceMatch.Success)
        {
            var raw = priceMatch.Groups["price"].Value.Replace(" ", "");
            var curr = priceMatch.Groups["currency"].Value.Trim().ToUpperInvariant();
            if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var p) && p > 0 && p < 100_000)
            {
                if (string.IsNullOrEmpty(curr)) curr = KrCurrencyForTld(tld);
                return new PriceInfo(p.ToString(CultureInfo.InvariantCulture), NormalizeCurrency(curr, tld));
            }
        }

        // 2. Visible text patterns: prefer explicit ranges.
        foreach (Match m in PriceRangeRegex().Matches(text))
        {
            var raw1 = m.Groups["num1"].Value.Replace(" ", "").Replace("\u00a0", "");
            var raw2 = m.Groups["num2"].Value.Replace(" ", "").Replace("\u00a0", "");
            if (!int.TryParse(raw1, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount1)
                || !int.TryParse(raw2, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount2))
            {
                continue;
            }

            if (amount1 < 10 || amount1 > 100_000 || amount2 < 10 || amount2 > 100_000)
                continue;

            var currencyToken = m.Groups["pre1"].Value.Trim();
            if (string.IsNullOrEmpty(currencyToken)) currencyToken = m.Groups["post1"].Value.Trim();
            if (string.IsNullOrEmpty(currencyToken)) currencyToken = m.Groups["pre2"].Value.Trim();
            if (string.IsNullOrEmpty(currencyToken)) currencyToken = m.Groups["post2"].Value.Trim();

            var currency = ResolveCurrency(currencyToken, currencyToken, tld);
            if (currency is null) continue;

            var min = Math.Min(amount1, amount2);
            var max = Math.Max(amount1, amount2);
            return new PriceInfo($"{min}-{max}", NormalizeCurrency(currency, tld));
        }

        // 3. Single price values.
        var best = (Amount: string.Empty, Currency: string.Empty);
        var bestScore = 0;

        foreach (Match m in PriceRegex().Matches(text))
        {
            var prefix = m.Groups["pre"].Value.Trim();
            var numRaw = m.Groups["num"].Value.Replace(" ", "").Replace("\u00a0", "");
            var suffix = m.Groups["post"].Value.Trim();

            if (!int.TryParse(numRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var amount))
                continue;
            if (amount < 10 || amount > 100_000) continue;

            var currency = ResolveCurrency(prefix, suffix, tld);
            if (currency is null) continue;

            // Prefer prices near registration keywords; among those, take the max amount.
            var idx = m.Index;
            var ctxStart = Math.Max(0, idx - 120);
            var ctxLen = Math.Min(text.Length - ctxStart, m.Length + 240);
            var ctx = text.AsSpan(ctxStart, ctxLen);
            var score = 1;
            if (HasPriceContext(ctx)) score += 5;
            if (amount >= 100) score += 1; // more likely a real entry fee

            if (score > bestScore || (score == bestScore && amount > int.Parse(best.Amount, CultureInfo.InvariantCulture)))
            {
                best = (amount.ToString(CultureInfo.InvariantCulture), NormalizeCurrency(currency, tld));
                bestScore = score;
            }
        }

        return bestScore > 0 ? new PriceInfo(best.Amount, best.Currency) : null;
    }

    private static bool HasPriceContext(ReadOnlySpan<char> ctx)
    {
        return ctx.Contains("pris", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("price", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("fee", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("avgift", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("anmälan", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("anmälning", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("registration", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("inscription", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("tarif", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("startavgift", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("p\u00e5melding", StringComparison.OrdinalIgnoreCase) // påmelding
            || ctx.Contains("deltaker", StringComparison.OrdinalIgnoreCase)
            || ctx.Contains("entry", StringComparison.OrdinalIgnoreCase);
    }

    private static string? ResolveCurrency(string prefix, string suffix, string? tld)
    {
        // Check prefix first (€, $, £, CHF, SEK, NOK, DKK, EUR, USD, GBP, EURO)
        var token = prefix.Length > 0 ? prefix : suffix;
        if (string.IsNullOrWhiteSpace(token)) return null;
        token = token.Trim('.', ',', ':').Trim();
        return token.ToUpperInvariant() switch
        {
            "KR" or "KR." => KrCurrencyForTld(tld),
            "SEK" => "SEK",
            "NOK" => "NOK",
            "DKK" => "DKK",
            "€" or "EUR" or "EURO" or "EUROS" => "EUR",
            "$" or "USD" => "USD",
            "£" or "GBP" => "GBP",
            "CHF" => "CHF",
            "ISK" => "ISK",
            _ => null
        };
    }

    private static string KrCurrencyForTld(string? tld) => tld switch
    {
        "no" => "NOK",
        "dk" => "DKK",
        "is" => "ISK",
        _ => "SEK", // .se, .com, and everything else
    };

    private static string NormalizeCurrency(string currency, string? tld = null) => currency.ToUpperInvariant() switch
    {
        "KR" or "KR." => KrCurrencyForTld(tld),
        "€" => "EUR",
        "$" => "USD",
        "£" => "GBP",
        "EURO" or "EUROS" => "EUR",
        _ => currency.ToUpperInvariant()
    };

    // Matches explicit price ranges like "60€ - 90€", "60 - 90 EUR", "€60 to €90".
    [GeneratedRegex(@"(?<pre1>[€$£]|(?:SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS|kr\.?)\s?)?(?<num1>\d[\d\s\u00a0]{0,6}\d|\d{1,5})(?:,-)?\s*(?<post1>€|kr\.?|SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS)?\s*(?:[-–—]|to|till|until)\s*(?<pre2>[€$£]|(?:SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS|kr\.?)\s?)?(?<num2>\d[\d\s\u00a0]{0,6}\d|\d{1,5})(?:,-)?\s*(?<post2>€|kr\.?|SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS)?", RegexOptions.IgnoreCase)]
    private static partial Regex PriceRangeRegex();

    // Matches price amounts with optional currency prefix/suffix.
    // "500 kr", "€50", "SEK 300", "1 200 NOK", "kr 500", "$120", "600,- NOK".
    [GeneratedRegex(@"(?<pre>[€$£]|(?:SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS|kr\.?)\s?)?(?<num>\d[\d\s\u00a0]{0,6}\d|\d{1,5})(?:,-)?\s*(?<post>€|kr\.?|SEK|NOK|DKK|EUR|USD|GBP|CHF|ISK|EURO|EUROS)?", RegexOptions.IgnoreCase)]
    private static partial Regex PriceRegex();

    // Matches JSON-LD "price" and "priceCurrency" in offers.
    [GeneratedRegex(@"""price""\s*:\s*""?(?<price>\d[\d\s]{0,6}\d|\d{1,5})""?.*?""priceCurrency""\s*:\s*""(?<currency>[A-Z]{3})""", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex JsonLdPriceRegex();

}
