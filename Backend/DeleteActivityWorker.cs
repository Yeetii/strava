using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class DeleteActivityWorker(
    ILogger<DeleteActivityWorker> _logger,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    CollectionClient<VisitedPath> _visitedPathsCollection,
    CollectionClient<VisitedArea> _visitedAreasCollection,
    ServiceBusClient _serviceBusClient,
    ServiceBusAdministrationClient serviceBusAdministrationClient)
{
    [Function(nameof(DeleteActivityWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.ActivityDeleteJobs, Connection = "ServiceBusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var deleteJob = message.Body.ToObjectFromJson<ActivityDeleteJob>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        if (deleteJob == null)
            throw new InvalidOperationException("Activity delete job payload is invalid.");

        if (await ServiceBusRescheduler.TryDeferForBackpressureAsync(
                serviceBusAdministrationClient,
                _serviceBusClient,
                ServiceBusConfig.ActivityDeleteJobs,
                message,
                actions,
                _logger,
                cancellationToken))
        {
            return;
        }

        try
        {
            await RemoveActivityFromLinkedDocuments(_summitedPeaksCollection, deleteJob.UserId, deleteJob.ActivityId, cancellationToken);
            await RemoveActivityFromLinkedDocuments(_visitedPathsCollection, deleteJob.UserId, deleteJob.ActivityId, cancellationToken);
            await RemoveActivityFromLinkedDocuments(_visitedAreasCollection, deleteJob.UserId, deleteJob.ActivityId, cancellationToken);
            await DeleteActivityIfPresent(deleteJob, cancellationToken);

            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex)
        {
            if (ex is CosmosException { StatusCode: System.Net.HttpStatusCode.TooManyRequests })
                ServiceBusRescheduler.RecordCosmosThrottle();

            await ServiceBusRescheduler.HandleRetryAsync(
                ex,
                actions,
                message,
                _serviceBusClient,
                ServiceBusConfig.ActivityDeleteJobs,
                _logger,
                cancellationToken);
        }
    }

    private async Task DeleteActivityIfPresent(ActivityDeleteJob deleteJob, CancellationToken cancellationToken)
    {
        try
        {
            await _activitiesCollection.DeleteDocument(deleteJob.ActivityId, new PartitionKey(deleteJob.UserId), cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            _logger.LogInformation(
                "Activity {ActivityId} for user {UserId} was already deleted or not found.",
                deleteJob.ActivityId,
                deleteJob.UserId);
        }
    }

    private static async Task RemoveActivityFromLinkedDocuments<T>(
        CollectionClient<T> collection,
        string userId,
        string activityId,
        CancellationToken cancellationToken)
        where T : IDocument
    {
        var partitionKey = new PartitionKey(userId);
        var linkedDocs = await GetActivityLinkedDocuments(collection, userId, activityId, cancellationToken);
        var cleanupPlan = BuildCleanupPlan(linkedDocs, activityId);

        foreach (var docId in cleanupPlan.DocumentIdsToDelete)
        {
            await DeleteLinkedDocumentIfPresent(collection, docId, partitionKey, cancellationToken);
        }

        foreach (var patch in cleanupPlan.Patches)
        {
            await PatchLinkedDocumentIfPresent(collection, patch, partitionKey, cancellationToken);
        }
    }

    private static async Task DeleteLinkedDocumentIfPresent<T>(
        CollectionClient<T> collection,
        string documentId,
        PartitionKey partitionKey,
        CancellationToken cancellationToken)
        where T : IDocument
    {
        try
        {
            await collection.DeleteDocument(documentId, partitionKey, cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
        }
    }

    private static async Task PatchLinkedDocumentIfPresent<T>(
        CollectionClient<T> collection,
        ActivityIdsPatch patch,
        PartitionKey partitionKey,
        CancellationToken cancellationToken)
        where T : IDocument
    {
        try
        {
            await collection.PatchDocument(
                patch.DocumentId,
                partitionKey,
                [PatchOperation.Set("/activityIds", patch.RemainingActivityIds)],
                cancellationToken: cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
        }
    }

    internal static CleanupPlan BuildCleanupPlan(
        IEnumerable<ActivityLinkedDocumentProjection> linkedDocs,
        string activityId)
    {
        var documentIdsToDelete = new List<string>();
        var patches = new List<ActivityIdsPatch>();

        foreach (var doc in linkedDocs)
        {
            var remainingActivityIds = (doc.ActivityIds ?? [])
                .Where(id => !string.Equals(id, activityId, StringComparison.Ordinal))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            if (remainingActivityIds.Count == 0)
            {
                documentIdsToDelete.Add(doc.Id);
                continue;
            }

            patches.Add(new ActivityIdsPatch(doc.Id, remainingActivityIds));
        }

        return new CleanupPlan(documentIdsToDelete, patches);
    }

    private static async Task<List<ActivityLinkedDocumentProjection>> GetActivityLinkedDocuments<T>(
        CollectionClient<T> collection,
        string userId,
        string activityId,
        CancellationToken cancellationToken)
        where T : IDocument
    {
        var query = new QueryDefinition(@"
SELECT c.id, c.activityIds
FROM c
WHERE c.userId = @userId
  AND ARRAY_CONTAINS(c.activityIds, @activityId)")
            .WithParameter("@userId", userId)
            .WithParameter("@activityId", activityId);

        return (await collection.ExecuteQueryAsync<ActivityLinkedDocumentProjection>(query, cancellationToken: cancellationToken)).ToList();
    }

    internal sealed class ActivityLinkedDocumentProjection
    {
        public required string Id { get; set; }
        public HashSet<string>? ActivityIds { get; set; }
    }

    internal sealed record ActivityIdsPatch(string DocumentId, IReadOnlyList<string> RemainingActivityIds);

    internal sealed record CleanupPlan(
        IReadOnlyList<string> DocumentIdsToDelete,
        IReadOnlyList<ActivityIdsPatch> Patches);
}
