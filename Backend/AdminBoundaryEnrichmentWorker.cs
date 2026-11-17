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
    private static readonly TimeSpan InvocationSafetyBuffer = TimeSpan.FromSeconds(75);
    /// <summary>How often to renew the Service Bus lock while a single boundary is being enriched.</summary>
    private static readonly TimeSpan LockRenewalInterval = TimeSpan.FromSeconds(240);

    [Function(nameof(AdminBoundaryEnrichmentWorker))]
    public async Task Run(
        [QueueTrigger(QueueConfig.EnrichAdminBoundaryJobs, IsBatched = true, AutoCompleteMessages = false)]
        QueueMessage[] messages,
        QueueMessageActions actions,
        CancellationToken cancellationToken)
    {
        // Renew all locks immediately — messages may have been sitting in the Functions trigger's
        // prefetch buffer while a prior invocation ran, leaving little lock time remaining.
        // This must happen before backpressure deferral and overflow deferral, both of which need
        // valid locks to complete/schedule messages.
        await RenewLocksAsync(messages, actions, logger, cancellationToken);


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
            logger.LogError(ex, "[AdminBoundaryEnrichmentWorker] Failed to load admin boundary documents");
            throw;
        }

        var documentsById = documents.ToDictionary(document => document.Id, StringComparer.Ordinal);

        var patches = new List<(string Id, PartitionKey PartitionKey, IReadOnlyList<PatchOperation> Operations)>();
        var messagesToCompleteAfterPatch = new List<QueueMessage>();
        var patchedIds = new HashSet<string>(StringComparer.Ordinal);

        for (var index = 0; index < messages.Length; index++)
        {
            var message = messages[index];
            if (DateTimeOffset.UtcNow >= invocationDeadlineUtc)
            {
                // Leave the remaining messages unsettled so Service Bus redelivers
                // them after their lock expires (no rescheduler to defer them).
                logger.LogWarning("[AdminBoundaryEnrichmentWorker] Invocation time budget reached; {Remaining} messages left unsettled for redelivery", messages.Length - index);
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
                logger.LogError(ex, "[AdminBoundaryEnrichmentWorker] Failed to enrich admin boundary {BoundaryId}", boundaryId);
                throw;
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
                logger.LogError(ex, "[AdminBoundaryEnrichmentWorker] Failed to persist admin boundary patches");
                throw;
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
        QueueMessage message,
        QueueMessageActions actions,
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
                    if (renewalCts.Token.IsCancellationRequested || !QueueMessageGuard.HasLockToken(message))
                        break;
                    await actions.RenewMessageLockAsync(message, cancellationToken);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex) when (ex is QueueException { Reason: QueueExceptionReason.MessageLockLost }
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
        IEnumerable<QueueMessage> messages,
        QueueMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        foreach (var message in messages.Where(QueueMessageGuard.HasLockToken))
        {
            try
            {
                await actions.RenewMessageLockAsync(message, cancellationToken);
            }
            catch (Exception ex) when (ex is QueueException { Reason: QueueExceptionReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
            {
                logger.LogWarning("[AdminBoundaryEnrichmentWorker] Lock lost before continuing admin boundary enrichment for message {MessageId}; it will be redelivered.", message.MessageId);
            }
        }
    }

    internal static async Task TryCompleteMessageAsync(
        QueueMessage message,
        QueueMessageActions actions,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (!QueueMessageGuard.HasLockToken(message))
            return;

        try
        {
            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex) when (ex is QueueException { Reason: QueueExceptionReason.MessageLockLost } || ex.Message.Contains("MessageLockLost"))
        {
            logger.LogWarning(ex,
                "[AdminBoundaryEnrichmentWorker] Message lock already lost while completing admin boundary enrichment message {MessageId}; message will be redelivered.",
                message.MessageId);
        }
    }
}
