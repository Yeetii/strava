using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class EnrichNewAdminBoundaries(
    AdminBoundaryMetricsEnricher enricher,
    CollectionClient<StoredFeature> storedFeaturesCollection,
    ILogger<EnrichNewAdminBoundaries> logger)
{
    private readonly AdminBoundaryMetricsEnricher _enricher = enricher;
    private readonly CollectionClient<StoredFeature> _storedFeaturesCollection = storedFeaturesCollection;
    private readonly ILogger<EnrichNewAdminBoundaries> _logger = logger;

    [Function(nameof(EnrichNewAdminBoundaries))]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: "%CosmosDb%",
            containerName: DatabaseConfig.OsmFeaturesContainer,
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "adminBoundaryMetrics",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<StoredFeature> changedDocuments,
        CancellationToken cancellationToken)
    {
        foreach (var document in changedDocuments)
        {
            if (!ShouldEnrich(document))
            {
                continue;
            }

            _logger.LogInformation("Enriching admin boundary {BoundaryId}", document.Id);
            await _enricher.EnrichAsync(document, cancellationToken);
            await _storedFeaturesCollection.UpsertDocument(document, cancellationToken);
        }
    }

    internal static bool ShouldEnrich(StoredFeature document)
    {
        if (!string.Equals(document.Kind, FeatureKinds.AdminBoundary, StringComparison.Ordinal))
        {
            return false;
        }

        if (document.Id.StartsWith("empty-", StringComparison.Ordinal))
        {
            return false;
        }

        if (document.Properties.TryGetValue("adminBoundaryMetricsVersion", out var version))
        {
            var versionText = version?.ToString();
            if (int.TryParse(versionText, out int parsedVersion)
                && parsedVersion >= AdminBoundaryMetricsEnricher.MetricsVersion)
            {
                return false;
            }
        }

        return true;
    }
}