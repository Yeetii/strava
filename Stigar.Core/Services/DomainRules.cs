using Stigar.Core.Models;

namespace Stigar.Core.Services;

/// <summary>Rules shared by the API's admin endpoints and the Workers enrichment jobs.</summary>
public static class AdminBoundaryEnrichmentRules
{
    /// <summary>Message property set when enrichment was triggered by new feature data.</summary>
    public const string ForceEnrichProperty = "ForceEnrich";

    public static bool ShouldEnrich(StoredFeature document)
    {
        if (!string.Equals(document.Kind, FeatureKinds.AdminBoundary, StringComparison.Ordinal))
            return false;

        if (document.Id.StartsWith("empty-", StringComparison.Ordinal))
            return false;

        if (StoredFeature.IsPointerDocument(document))
            return false;

        foreach (var key in new[] { "adminBoundaryMetricsVersion", "metricsVersion" })
        {
            if (!document.Properties.TryGetValue(key, out var version))
                continue;

            if (int.TryParse(version?.ToString(), out int parsedVersion)
                && parsedVersion >= AdminBoundaryMetricsEnricher.MetricsVersion)
            {
                return false;
            }
        }

        return true;
    }
}

public static class PeakIdRules
{
    public static string Normalize(BAMCIS.GeoJSON.FeatureId peakId)
        => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, peakId.Value);

    /// <summary>
    /// The peaks table is keyed by the kind-prefixed stored id (for example <c>peak:11908635</c>).
    /// Junction rows that reference it must use that exact format or the FK rejects them.
    /// </summary>
    public static string ToStoredPeakId(BAMCIS.GeoJSON.FeatureId peakId)
        => StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.Peak, Normalize(peakId));

    public static string BuildSummitedPeakDocumentId(string userId, BAMCIS.GeoJSON.FeatureId peakId)
        => $"{userId}-{Normalize(peakId)}";
}
