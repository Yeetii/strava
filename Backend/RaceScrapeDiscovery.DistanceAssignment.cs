using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Shared.Services;

namespace Backend;

public static partial class RaceScrapeDiscovery
{
    /// <inheritdoc cref="RaceDistanceKm.TryParseMarathonKeyword" />
    public static bool TryParseMarathonKeyword(string token, out double km) =>
        RaceDistanceKm.TryParseMarathonKeyword(token, out km);

    // Parses a verbose distance string into a list of (km, formatted) pairs.
    // Marathon keywords are translated; non-parseable tokens are skipped.
    private static IReadOnlyList<(double Km, string Formatted)> ParseVerboseDistanceParts(string distanceVerbose)
    {
        var parts = DistanceListSeparatorRegex
            .Split(distanceVerbose)
            .Select(part => part.Trim())
            .Where(part => part.Length > 0);

        var result = new List<(double, string)>();
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

}
