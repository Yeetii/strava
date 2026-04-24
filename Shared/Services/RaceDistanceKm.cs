using System.Globalization;
using System.Text.RegularExpressions;

namespace Shared.Services;

/// <summary>
/// Shared kilometre parsing and distance comparison for race assembly, scraping, and HTML helpers.
/// </summary>
public static class RaceDistanceKm
{
    /// <summary>Default relative tolerance for symmetric “roughly the same ultra” checks (assembly).</summary>
    public const double DefaultSymmetricRoughFraction = 0.03;

    /// <summary>Statute miles to kilometres (same factor as <c>RaceScrapeDiscovery</c> verbose parsing).</summary>
    public const double StatuteMilesToKm = 1.60934;

    private static readonly CultureInfo Inv = CultureInfo.InvariantCulture;
    private static readonly NumberStyles Float = NumberStyles.Float;

    /// <summary>
    /// Trailing unit: meter(s), metre(s), miles, mi, km, k (case-insensitive). Excludes bare <c>m</c> and capital <c>M</c>
    /// — those are handled separately (metres vs statute miles).
    /// </summary>
    private static readonly Regex WordOrCompactUnitSuffix = new(
        @"(?i)(?<suffix>meters?|metres?|miles?|mi|km|k)\s*$",
        RegexOptions.Compiled);

    /// <summary>Statute miles when the suffix is an ASCII capital <c>M</c> after the number (e.g. <c>10M</c>).</summary>
    private static readonly Regex StatuteMilesCapitalM = new(
        @"^(?<v>[\d.]+)\s*M\s*$",
        RegexOptions.Compiled);

    /// <summary>Metres when the suffix is a lowercase <c>m</c> after the number (e.g. <c>400m</c>).</summary>
    private static readonly Regex MetresLowercaseM = new(
        @"^(?<v>[\d.]+)\s*m\s*$",
        RegexOptions.Compiled);

    /// <summary>
    /// True when <paramref name="aKm"/> and <paramref name="bKm"/> differ by strictly less than
    /// <paramref name="fractionOfLonger"/> times the larger value (e.g. 3% → 509 vs 511).
    /// </summary>
    public static bool RoughlyEqualSymmetric(double aKm, double bKm, double fractionOfLonger = DefaultSymmetricRoughFraction)
    {
        var diff = Math.Abs(aKm - bKm);
        if (diff == 0) return true;
        var longer = Math.Max(aKm, bKm);
        if (longer <= 0) return false;
        return diff < fractionOfLonger * longer;
    }

    /// <summary>
    /// True when |<paramref name="referenceKm"/> − <paramref name="otherKm"/>| ≤
    /// <paramref name="maxRelativeError"/> × <paramref name="referenceKm"/> (reference must be &gt; 0).
    /// Used for GPX-vs-published distance assignment (e.g. 25% band).
    /// </summary>
    public static bool WithinRelativeOfReference(double referenceKm, double otherKm, double maxRelativeError)
    {
        if (referenceKm <= 0) return false;
        return Math.Abs(referenceKm - otherKm) <= maxRelativeError * referenceKm;
    }

    /// <summary>
    /// True if <paramref name="token"/> is a marathon / half-marathon keyword; sets <paramref name="km"/> to 42 or 21.
    /// </summary>
    public static bool TryParseMarathonKeyword(string token, out double km)
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

    /// <summary>Parses the first comma-separated segment to kilometres.</summary>
    public static double? TryParsePrimarySegmentKilometers(string? distance)
    {
        if (string.IsNullOrWhiteSpace(distance)) return null;
        var first = distance.Split(',')[0].Trim();
        return TryParseSingleDistanceTokenToKm(first, out var km) ? km : null;
    }

    /// <summary>
    /// Parses one trimmed list token: marathon keywords, metric/imperial units (<c>M</c> = miles, <c>m</c> = metres),
    /// <c>meters</c>/<c>metres</c>/<c>meter</c>/<c>mi</c>/<c>km</c>/<c>k</c>, or a bare number as km.
    /// </summary>
    public static bool TryParseCommaListTokenKilometers(string trimmedToken, out double km) =>
        TryParseSingleDistanceTokenToKm(trimmedToken, out km);

    /// <summary>
    /// Comma-separated segments; skips blanks and unparseable tokens.
    /// </summary>
    public static List<double> ParseCommaSeparatedKilometers(string? distance)
    {
        if (string.IsNullOrWhiteSpace(distance)) return [];

        var result = new List<double>();
        foreach (var token in distance.Split(','))
        {
            var trimmed = token.Trim();
            if (trimmed.Length == 0) continue;
            if (TryParseCommaListTokenKilometers(trimmed, out var km))
                result.Add(km);
        }
        return result;
    }

    private static bool TryParseSingleDistanceTokenToKm(string token, out double km)
    {
        if (TryParseMarathonKeyword(token, out km))
            return true;

        var wordMatch = WordOrCompactUnitSuffix.Match(token);
        if (wordMatch.Success)
        {
            var stripped = WordOrCompactUnitSuffix.Replace(token, "").Trim();
            if (!double.TryParse(stripped, Float, Inv, out var value))
            {
                km = 0;
                return false;
            }

            var u = wordMatch.Groups["suffix"].Value.ToLowerInvariant();
            km = u switch
            {
                "mi" or "mile" or "miles" => value * StatuteMilesToKm,
                "meter" or "meters" or "metre" or "metres" => value / 1000.0,
                _ => value, // km, k
            };
            return true;
        }

        var capM = StatuteMilesCapitalM.Match(token);
        if (capM.Success)
        {
            if (!double.TryParse(capM.Groups["v"].Value, Float, Inv, out var miles))
            {
                km = 0;
                return false;
            }

            km = miles * StatuteMilesToKm;
            return true;
        }

        var lowM = MetresLowercaseM.Match(token);
        if (lowM.Success)
        {
            if (!double.TryParse(lowM.Groups["v"].Value, Float, Inv, out var metres))
            {
                km = 0;
                return false;
            }

            km = metres / 1000.0;
            return true;
        }

        return double.TryParse(token.Trim(), Float, Inv, out km);
    }
}
