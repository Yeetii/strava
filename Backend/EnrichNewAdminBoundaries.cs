using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class EnrichNewAdminBoundaries(
    ServiceBusClient serviceBusClient,
    ILogger<EnrichNewAdminBoundaries> logger)
{
    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.EnrichAdminBoundaryJobs);

    [Function(nameof(EnrichNewAdminBoundaries))]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: "DatabaseConfig.CosmosDb",
            containerName: DatabaseConfig.OsmFeaturesContainer,
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "adminBoundaryMetrics",
            CreateLeaseContainerIfNotExists = true)] IReadOnlyList<StoredFeature> changedDocuments,
        CancellationToken cancellationToken)
    {
        var jobs = changedDocuments
            .Where(ShouldEnrich)
            .Select(d => new ServiceBusMessage(d.Id))
            .ToList();

        if (jobs.Count == 0)
            return;

        logger.LogInformation("Queuing {Count} admin boundary enrichment jobs", jobs.Count);
        await _sender.SendMessagesAsync(jobs, cancellationToken);
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
}
