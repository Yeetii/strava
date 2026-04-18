using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class AdminBoundaryEnrichmentWorker(
    AdminBoundaryMetricsEnricher enricher,
    CollectionClient<StoredFeature> storedFeaturesCollection,
    ILogger<AdminBoundaryEnrichmentWorker> logger)
{
    [Function(nameof(AdminBoundaryEnrichmentWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.EnrichAdminBoundaryJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] messages,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var ids = messages.Select(m => m.Body.ToString()).ToList();
        logger.LogInformation("Processing {Count} admin boundary enrichment jobs", ids.Count);

        var documents = await storedFeaturesCollection.GetByIdsAsync(ids);

        foreach (var document in documents)
        {
            var message = messages.First(m => m.Body.ToString() == document.Id);
            try
            {
                logger.LogInformation("Enriching admin boundary {BoundaryId}", document.Id);
                var ops = await enricher.CalculatePatchOperationsAsync(document, cancellationToken);
                var pk = new PartitionKeyBuilder().Add((double)document.X).Add((double)document.Y).Build();
                await storedFeaturesCollection.PatchDocument(document.Id, pk, ops, cancellationToken);
                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to enrich admin boundary {BoundaryId}", document.Id);
                await actions.DeadLetterMessageAsync(message,
                    deadLetterReason: nameof(AdminBoundaryEnrichmentWorker),
                    deadLetterErrorDescription: $"Boundary {document.Id}: {ex.Message}",
                    cancellationToken: cancellationToken);
            }
        }
    }
}
