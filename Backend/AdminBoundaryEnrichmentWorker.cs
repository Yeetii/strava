using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
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
    ServiceBusClient serviceBusClient,
    ServiceBusAdministrationClient serviceBusAdministrationClient,
    ILogger<AdminBoundaryEnrichmentWorker> logger)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    private const int MaxMessagesPerRun = 6;
    private static readonly TimeSpan InvocationSafetyBuffer = TimeSpan.FromSeconds(75);
    private static readonly TimeSpan SpilloverDelay = TimeSpan.FromMinutes(1);

    [Function(nameof(AdminBoundaryEnrichmentWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.EnrichAdminBoundaryJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] messages,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        if (await ServiceBusRescheduler.TryDeferForBackpressureAsync(
                serviceBusAdministrationClient,
                _serviceBusClient,
                ServiceBusConfig.EnrichAdminBoundaryJobs,
                messages,
                actions,
                logger,
                cancellationToken))
        {
            return;
        }

        var processMessages = messages
            .Take(MaxMessagesPerRun)
            .ToList();
        var overflowMessages = messages
            .Skip(MaxMessagesPerRun)
            .ToList();

        if (overflowMessages.Count > 0)
        {
            await ServiceBusRescheduler.DeferMessagesAsync(
                _serviceBusClient,
                ServiceBusConfig.EnrichAdminBoundaryJobs,
                overflowMessages,
                actions,
                logger,
                cancellationToken,
                SpilloverDelay,
                "batch-size-limit");
        }

        if (processMessages.Count == 0)
            return;

        var invocationDeadlineUtc = DateTimeOffset.UtcNow.AddMinutes(10) - InvocationSafetyBuffer;

        var ids = processMessages
            .Select(m => m.Body.ToString())
            .ToList();
        logger.LogInformation("Processing {Count} admin boundary enrichment jobs ({TotalReceived} received)", ids.Count, messages.Length);

        var documents = await storedFeaturesCollection.GetByIdsAsync(ids.Distinct(StringComparer.Ordinal), cancellationToken);
        var documentsById = documents.ToDictionary(document => document.Id, StringComparer.Ordinal);

        var patches = new List<(string Id, PartitionKey PartitionKey, IReadOnlyList<PatchOperation> Operations)>();
        var patchedIds = new HashSet<string>(StringComparer.Ordinal);

        foreach (var message in processMessages)
        {
            if (DateTimeOffset.UtcNow >= invocationDeadlineUtc)
            {
                var spillover = processMessages
                    .SkipWhile(m => m != message)
                    .ToList();

                await ServiceBusRescheduler.DeferMessagesAsync(
                    _serviceBusClient,
                    ServiceBusConfig.EnrichAdminBoundaryJobs,
                    spillover,
                    actions,
                    logger,
                    cancellationToken,
                    SpilloverDelay,
                    "invocation-time-budget");

                break;
            }

            var boundaryId = message.Body.ToString();

            if (!documentsById.TryGetValue(boundaryId, out var document))
            {
                logger.LogWarning("Admin boundary document {BoundaryId} not found; completing message {MessageId}", boundaryId, message.MessageId);
                await actions.CompleteMessageAsync(message, cancellationToken);
                continue;
            }

            if (patchedIds.Contains(boundaryId))
            {
                await actions.CompleteMessageAsync(message, cancellationToken);
                continue;
            }

            try
            {
                logger.LogInformation("Enriching admin boundary {BoundaryId}", document.Id);
                var ops = await enricher.CalculatePatchOperationsAsync(document, cancellationToken);
                var pk = new PartitionKeyBuilder().Add((double)document.X).Add((double)document.Y).Build();
                patches.Add((document.Id, pk, ops));
                patchedIds.Add(boundaryId);
            }
            catch (Exception ex)
            {
                if (ex is CosmosException { StatusCode: System.Net.HttpStatusCode.TooManyRequests })
                {
                    ServiceBusRescheduler.RecordCosmosThrottle();
                }

                await ServiceBusRescheduler.HandleRetryAsync(
                    ex, actions, message, _serviceBusClient, ServiceBusConfig.EnrichAdminBoundaryJobs, logger, cancellationToken);
                continue;
            }
        }

        if (patches.Count > 0)
        {
            await storedFeaturesCollection.PatchDocuments(patches, cancellationToken: cancellationToken);
        }

        foreach (var message in processMessages)
        {
            var boundaryId = message.Body.ToString();
            if (!patchedIds.Contains(boundaryId))
                continue;

            await actions.CompleteMessageAsync(message, cancellationToken);
        }
    }
}
