using Shared.Models;
using Shared.Services;

namespace Backend;

/// <summary>
/// Distance helpers for <see cref="AssembleRaceWorker"/> — all forwarded to <see cref="RaceAssembler"/>.
/// </summary>
partial class AssembleRaceWorker
{
    public static bool DistancesRoughMatchKm(double aKm, double bKm)
        => RaceAssembler.DistancesRoughMatchKm(aKm, bKm);

    public static SourceDiscovery FindBestDiscoveryForRoute(
        double? routeKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> orderedDiscoveries,
        SourceDiscovery fallback)
        => RaceAssembler.FindBestDiscoveryForRoute(routeKm, orderedDiscoveries, fallback);

    public static List<double> ParseDistanceList(string? distance)
        => RaceAssembler.ParseDistanceList(distance);

    public static double? ParseDistanceKm(string? distance)
        => RaceAssembler.ParseDistanceKm(distance);

    internal static void ClaimRoughlyMatchingDiscoveryDistances(
        double effectiveRouteKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries,
        HashSet<double> claimedKm)
        => RaceAssembler.ClaimRoughlyMatchingDiscoveryDistances(effectiveRouteKm, flatDiscoveries, claimedKm);

    internal static bool AssemblyAlreadyCoversRoughDistanceKm(double km, IReadOnlyList<StoredFeature> results)
        => RaceAssembler.AssemblyAlreadyCoversRoughDistanceKm(km, results);

    internal static List<(double? Km, string Label)> GetUnclaimedDiscoveryDistances(
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries,
        HashSet<double> claimedKm)
        => RaceAssembler.GetUnclaimedDiscoveryDistances(flatDiscoveries, claimedKm);

    internal static Dictionary<double, double> BuildCanonicalRouteKmMap(List<double> sortedDistinctKm)
        => RaceAssembler.BuildCanonicalRouteKmMap(sortedDistinctKm);

    internal static int CountRedundantMeasurementSingletons(
        double canonicalRouteKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries)
        => RaceAssembler.CountRedundantMeasurementSingletons(canonicalRouteKm, flatDiscoveries);

    internal static int CountRoughMatchingDiscoveryRouteSlots(
        double canonicalRouteKm,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries)
        => RaceAssembler.CountRoughMatchingDiscoveryRouteSlots(canonicalRouteKm, flatDiscoveries);

    internal static List<StoredFeature> MergeSameDistanceFeatures(List<StoredFeature> results)
        => RaceAssembler.MergeSameDistanceFeatures(results);

    internal static void AppendDistanceToAmbiguousNames(List<StoredFeature> results)
        => RaceAssembler.AppendDistanceToAmbiguousNames(results);
}
