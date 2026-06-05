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
    private static readonly TimeSpan InvocationSafetyBuffer = TimeSpan.FromSeconds(75);
    private static readonly TimeSpan SpilloverDelay = TimeSpan.FromMinutes(1);
    /// <summary>How often to renew the Service Bus lock while a single boundary is being enriched.</summary>
    private static readonly TimeSpan LockRenewalInterval = TimeSpan.FromSeconds(240);

    [Function(nameof(AdminBoundaryEnrichmentWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.EnrichAdminBoundaryJobs, Connection = "ServicebusConnection", IsBatched = true, AutoCompleteMessages = false)]
        ServiceBusReceivedMessage[] messages,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        // Renew all locks immediately — messages may have been sitting in the Functions trigger's
        // prefetch buffer while a prior invocation ran, leaving little lock time remaining.
        // This must happen before backpressure deferral and overflow deferral, both of which need
        // valid locks to complete/schedule messages.
        await RenewLocksAsync(messages, actions, logger, cancellationToken);

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

        var invocationDeadlineUtc = DateTimeOffset.UtcNow.AddMinutes(10) - InvocationSafetyBuffer;

        var ids = messages
            .Select(m => m.Body.ToString())
            .ToList();
        logger.LogInformation("[AdminBoundaryEnrichmentWorker] Processing {Count} admin boundary enrichment jobs ({TotalReceived} received)", ids.Count, messages.Length);

        await RenewLocksAsync(messages, actions, logger, cancellationToken);

        IEnumerable<StoredFeature> documents;
        try
        {
            documents = await storedFeaturesCollection.GetByIdsAsync(ids.Distinct(StringComparer.Ordinal), cancellationToken);
        }
        catch (Exception ex)
        {
            if (ex is CosmosException { StatusCode: System.Net.HttpStatusCode.TooManyRequests })
            {
                ServiceBusRescheduler.RecordCosmosThrottle();
            }

            await RetryMessagesAsync(ex, messages, actions, logger, cancellationToken);
            return;
        }

        var documentsById = documents.ToDictionary(document => document.Id, StringComparer.Ordinal);

        var patches = new List<(string Id, PartitionKey PartitionKey, IReadOnlyList<PatchOperation> Operations)>();
        var messagesToCompleteAfterPatch = new List<ServiceBusReceivedMessage>();
        var patchedIds = new HashSet<string>(StringComparer.Ordinal);

        for (var index = 0; index < messages.Length; index++)
        {
            var message = messages[index];
            if (DateTimeOffset.UtcNow >= invocationDeadlineUtc)
            {
                var spillover = messages
                    .Skip(index)
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
                logger.LogWarning("[AdminBoundaryEnrichmentWorker] Admin boundary document {BoundaryId} not found; completing message {MessageId}", boundaryId, message.MessageId);
                await TryCompleteMessageAsync(message, actions, logger, cancellationToken);
                continue;
            }

            var forceEnrich = message.ApplicationProperties.TryGetValue(OsmFeaturesChangeTrigger.ForceEnrichProperty, out var forceVal)
                && forceVal is true;

            if (!forceEnrich && !OsmFeaturesChangeTrigger.ShouldEnrich(document))
            {
                logger.LogInformation("[AdminBoundaryEnrichmentWorker] Skipping already-enriched admin boundary {BoundaryId}", boundaryId);
                await TryCompleteMessageAsync(message, actions, logger, cancellationToken);
                continue;
            }

            if (patchedIds.Contains(boundaryId))
            {
                await TryCompleteMessageAsync(message, actions, logger, cancellationToken);
                continue;
            }

            try
            {
                logger.LogInformation("[AdminBoundaryEnrichmentWorker] Enriching admin boundary {BoundaryId}", document.Id);
                var ops = await RunWithPeriodicLockRenewalAsync(
                    message, actions, logger, LockRenewalInterval,
                    ct => enricher.CalculatePatchOperationsAsync(document, ct),
                    cancellationToken);
                var pk = new PartitionKeyBuilder().Add((double)document.X).Add((double)document.Y).Build();
                patches.Add((document.Id, pk, ops));
                messagesToCompleteAfterPatch.Add(message);
                patchedIds.Add(boundaryId);
            }
            catch (Exception ex)
            {
                if (ex is CosmosException { StatusCode: System.Net.HttpStatusCode.TooManyRequests })
                {
                    ServiceBusRescheduler.RecordCosmosThrottle();
                    await ServiceBusRescheduler.HandleRetryAsync(
                        ex, actions, message, _serviceBusClient, ServiceBusConfig.EnrichAdminBoundaryJobs, logger, cancellationToken);

                    var remaining = messages
                        .Skip(index + 1)
                        .ToList();
                    if (remaining.Count > 0)
                    {
                        await ServiceBusRescheduler.DeferMessagesAsync(
                            _serviceBusClient,
                            ServiceBusConfig.EnrichAdminBoundaryJobs,
                            remaining,
                            actions,
                            logger,
                            cancellationToken,
                            SpilloverDelay,
                            "cosmos-throttle");
                    }

                    break;
                }

                await ServiceBusRescheduler.HandleRetryAsync(
                    ex, actions, message, _serviceBusClient, ServiceBusConfig.EnrichAdminBoundaryJobs, logger, cancellationToken);
                continue;
            }
        }

        if (patches.Count > 0)
        {
            try
            {
                await RenewLocksAsync(messagesToCompleteAfterPatch, actions, logger, cancellationToken);
                await storedFeaturesCollection.PatchDocuments(patches, cancellationToken: cancellationToken);
            }
            catch (Exception ex)
            {
                if (ex is CosmosException { StatusCode: System.Net.HttpStatusCode.TooManyRequests })
                {
                    ServiceBusRescheduler.RecordCosmosThrottle();
                }

                await RetryMessagesAsync(ex, messagesToCompleteAfterPatch, actions, logger, cancellationToken);
                return;
            }
        }

        await RenewLocksAsync(messagesToCompleteAfterPatch, actions, logger, cancellationToken);
        foreach (var message in messagesToCompleteAfterPatch)
        {
            await TryCompleteMessageAsync(message, actions, logger, cancellationToken);
        }
    }

    /// <summary>
    /// Runs <paramref name="work"/> while renewing the Service Bus lock for
    /// <paramref name="message"/> every <paramref name="renewalInterval"/> in the background.
    /// The background renewal loop is cancelled as soon as the work completes.
    /// </summary>
    private static async Task<T> RunWithPeriodicLockRenewalAsync<T>(
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        ILogger logger,
        TimeSpan renewalInterval,
        Func<CancellationToken, Task<T>> work,
        CancellationToken cancellationToken)
    {
        using var renewalCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        var renewalTask = Task.Run(async () =>
        {
            while (!renewalCts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(renewalInterval, renewalCts.Token);
                    if (renewalCts.Token.IsCancellationRequested || !ServiceBusRescheduler.HasRealLockToken(message))
                        break;
                    await actions.RenewMessageLockAsync(message, cancellationToken);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost }
                                        || ex.Message.Contains("MessageLockLost"))
                {
                    logger.LogWarning("[AdminBoundaryEnrichmentWorker] SB lock lost mid-enrichment for message {MessageId}; message will be redelivered.", message.MessageId);
                    break;
                }
            }
        }, CancellationToken.None);

        try
        {
            return await work(cancellationToken);
        }
        finally
        {
            await renewalCts.CancelAsync();
            await renewalTask;
        }
    }

    internal static async Task RenewLocksAsync(
        IEnumerable<ServiceBusReceivedMessage> messages,
        ServiceBusMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        foreach (var message in messages.Where(ServiceBusRescheduler.HasRealLockToken))
        {
            try
            {
                await actions.RenewMessageLockAsync(message, cancellationToken);
            }
            catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                logger.LogWarning("[AdminBoundaryEnrichmentWorker] Lock lost before continuing admin boundary enrichment for message {MessageId}; it will be redelivered.", message.MessageId);
            }
        }
    }

    internal static async Task TryCompleteMessageAsync(
        ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (!ServiceBusRescheduler.HasRealLockToken(message))
            return;

        try
        {
            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex) when (ex is ServiceBusException { Reason: ServiceBusFailureReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
        {
            logger.LogWarning(ex,
                "[AdminBoundaryEnrichmentWorker] Message lock already lost while completing admin boundary enrichment message {MessageId}; message will be redelivered.",
                message.MessageId);
        }
    }

    private async Task RetryMessagesAsync(
        Exception exception,
        IReadOnlyList<ServiceBusReceivedMessage> messages,
        ServiceBusMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        foreach (var message in messages)
        {
            await ServiceBusRescheduler.HandleRetryAsync(
                exception,
                actions,
                message,
                _serviceBusClient,
                ServiceBusConfig.EnrichAdminBoundaryJobs,
                logger,
                cancellationToken);
        }
    }
}
