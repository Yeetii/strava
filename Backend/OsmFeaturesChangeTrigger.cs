using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class OsmFeaturesChangeTrigger(
    ServiceBusClient serviceBusClient,
    AdminBoundariesCollectionClient adminBoundaries,
    ILogger<OsmFeaturesChangeTrigger> logger)
{
    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.EnrichAdminBoundaryJobs);

    /// <summary>
    /// Service Bus message application property set when enrichment is triggered by new feature data
    /// rather than a schema version change. The worker bypasses the version check when this is present.
    /// </summary>
    internal const string ForceEnrichProperty = "ForceEnrich";

    [Function(nameof(OsmFeaturesChangeTrigger))]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: DatabaseConfig.CosmosDb,
            containerName: DatabaseConfig.OsmFeaturesContainer,
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "adminBoundaryMetrics",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<StoredFeature> changedDocuments,
        CancellationToken cancellationToken)
    {
        // Admin boundaries: enqueue for enrichment when their metrics version is stale.
        var adminBoundaryJobs = changedDocuments
            .Where(ShouldEnrich)
            .Select(d => new ServiceBusMessage(d.Id))
            .ToList();

        if (adminBoundaryJobs.Count > 0)
        {
            logger.LogInformation("Queuing {Count} admin boundary enrichment jobs", adminBoundaryJobs.Count);
            await _sender.SendMessagesAsync(adminBoundaryJobs, cancellationToken);
        }

        // Peaks / protected areas: find containing admin boundaries and force re-enrichment.
        var newFeatures = changedDocuments
            .Where(ShouldTriggerEnrichment)
            .ToList();

        if (newFeatures.Count == 0)
            return;

        var centroids = newFeatures
            .Select(f => f.Centroid!)
            .ToList();

        var containingBoundaries = (await adminBoundaries.FindBoundarySummariesContainingAnyPoint(centroids, cancellationToken))
            .DistinctBy(b => b.Id)
            .ToList();

        if (containingBoundaries.Count == 0)
            return;

            logger.LogInformation(
            "Queuing {BoundaryCount} admin boundary re-enrichment jobs triggered by {FeatureCount} new features",
            containingBoundaries.Count, newFeatures.Count);

        var forceJobs = containingBoundaries
            .Select(b =>
            {
                var msg = new ServiceBusMessage(b.Id);
                msg.ApplicationProperties[ForceEnrichProperty] = true;
                return msg;
            })
            .ToList();

        await _sender.SendMessagesAsync(forceJobs, cancellationToken);
    }

    internal static bool ShouldEnrich(StoredFeature document)
    {
        if (!string.Equals(document.Kind, FeatureKinds.AdminBoundary, StringComparison.Ordinal))
            return false;

        if (document.Id.StartsWith("empty-", StringComparison.Ordinal))
            return false;

        if (StoredFeature.IsPointerDocument(document))
            return false;

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

    internal static bool ShouldTriggerEnrichment(StoredFeature document)
    {
        if (document.Kind is not (FeatureKinds.Peak or FeatureKinds.ProtectedArea))
            return false;

        if (document.Id.StartsWith("empty-", StringComparison.Ordinal))
            return false;

        if (StoredFeature.IsPointerDocument(document))
            return false;

        // Centroid is required by the enricher — skip documents that haven't been backfilled yet.
        return document.Centroid != null;
    }
}
