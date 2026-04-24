namespace Shared.Services;

public static class RaceTypeNormalizer
{
    public static string? NormalizeRaceType(string? raceType)
    {
        if (string.IsNullOrWhiteSpace(raceType))
            return null;

        var parts = raceType
            .Split([',', ';', '/', '_'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.ToLowerInvariant())
            .Select(p => RaceTypeAliases.TryGetValue(p, out var mapped) ? mapped : p)
            .Where(p => p.Length > 0)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        // If any remaining token contains "trail" as a substring but "trail" isn't already a standalone token, prepend it.
        var hasTrailToken = parts.Contains("trail", StringComparer.Ordinal);
        var hasTrailWord = !hasTrailToken && parts.Any(p => p.Contains("trail", StringComparison.OrdinalIgnoreCase));
        if (hasTrailWord)
        {
            parts.Insert(0, "trail");
        }

        return parts.Count > 0 ? string.Join(", ", parts) : null;
    }

    public static IReadOnlyCollection<string> AliasKeys => RaceTypeAliases.Keys;

    private static readonly Dictionary<string, string> RaceTypeAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["stiløp"] = "trail", ["stig"] = "trail", ["trail race"] = "trail", 
        ["terreng"] = "cross country", ["terräng"] = "cross country", ["terrain"] = "cross country",
        ["terrengløp"] = "cross country", ["terränglopp"] = "cross country",
        ["asfalt"] = "road", ["landsväg"] = "road", ["gateløp"] = "road", ["gatlopp"] = "road", 
        ["väglopp"] = "road", ["vägløp"] = "road", ["road race"] = "road",
        ["grus"] = "gravel", ["grusväg"] = "gravel", ["grusløp"] = "gravel",
        ["gress"] = "grass", ["gräs"] = "grass",
        ["snø"] = "snow", ["snö"] = "snow", ["snowshoe"] = "snow", ["trugeløp"] = "snow", ["snöskor"] = "snow",
        ["stafett"] = "relay",
        ["motbakke"] = "uphill", ["vertical"] = "uphill", ["vertikal"] = "uphill",
        ["trappeløp"] = "stairs", ["trappor"] = "stairs", ["trapper"] = "stairs",
        ["hinderløp"] = "obstacle course", ["ocr"] = "obstacle course",
        ["baneløp"] = "track", ["barneløp"] = "kids",
        ["etappeløp"] = "stage race", ["timeløp"] = "timed race",
        ["triatlon"] = "triathlon", ["hundeløp"] = "canicross",
        ["randotrail"] = "trail", ["trailblanc"] = "trail",
        ["trailurbain"] = "urban trail",
        ["trail running"] = "trail",
        ["repeterende"] = "repeats",
        ["backyard ultra"] = "backyard",
        ["virtuelt"] = "virtual",
        ["halvmaraton"] = "", ["maraton"] = "", ["half marathon"] = "", ["marathon"] = "",
        ["rando"] = "", ["marchenordique"] = "", ["ultra"] = "", 
        ["courseroute"] = "", ["bane"] = "", ["3mi"] = "", ["start"] = "",  ["to"] = "",
        ["övrigt"] = "", ["other"] = "", ["annet"] = "", ["diverse"] = "", ["various"] = "",
    };
}
