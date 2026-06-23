using Microsoft.Azure.Cosmos;
using Shared.Models;
using System.Net;
using Microsoft.Extensions.Logging;

namespace Shared.Services;

/// <summary>
/// Holds a single write gate shared across all <see cref="CollectionClient{T}"/> instances and
/// type parameters so that concurrent workers cannot collectively saturate the Cosmos DB
/// free-tier RU/s budget.
/// </summary>
internal static class CosmosWriteThrottle
{
    internal static readonly SemaphoreSlim Semaphore = new(1, 1);
    internal static readonly TimeSpan DelayBetweenWrites = TimeSpan.FromMilliseconds(50);
    private const int PriorityPollDelayMs = 25;
    private static int _highPriorityWaiters;

    internal static async Task WaitForTurnAsync(CosmosWritePriority priority, CancellationToken cancellationToken)
    {
        if (priority == CosmosWritePriority.High)
        {
            Interlocked.Increment(ref _highPriorityWaiters);
            try
            {
                await Semaphore.WaitAsync(cancellationToken);
            }
            finally
            {
                Interlocked.Decrement(ref _highPriorityWaiters);
            }

            return;
        }

        // Best-effort prioritization: while urgent writes are queued, pause lower priority writers.
        while (Volatile.Read(ref _highPriorityWaiters) > 0)
            await Task.Delay(PriorityPollDelayMs, cancellationToken);

        await Semaphore.WaitAsync(cancellationToken);
    }
}

public enum CosmosWritePriority
{
    Low,
    Normal,
    High,
}

public class CollectionClient<T>(Container _container, ILoggerFactory loggerFactory) where T : IDocument
{
    protected readonly ILogger<CollectionClient<T>> _logger = loggerFactory.CreateLogger<CollectionClient<T>>();

    public sealed class RequestChargeAccumulator
    {
        public double TotalRequestCharge { get; private set; }

        internal void Add(double requestCharge) => TotalRequestCharge += requestCharge;
    }

    public sealed record MeasuredResult<TValue>(TValue Value, double RequestCharge);

    public async Task<IEnumerable<T>> FetchWholeCollection(CancellationToken cancellationToken = default)
    {
        return await ExecuteQueryAsync<T>(new QueryDefinition("SELECT * FROM p"), cancellationToken: cancellationToken);
    }

    public async Task<IEnumerable<S>> ExecuteQueryAsync<S>(QueryDefinition queryDefinition, QueryRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
        => await ExecuteQueryAsync<S>(queryDefinition, requestOptions, null, cancellationToken);

    protected async Task<IEnumerable<S>> ExecuteQueryAsync<S>(
        QueryDefinition queryDefinition,
        QueryRequestOptions? requestOptions,
        RequestChargeAccumulator? requestChargeAccumulator,
        CancellationToken cancellationToken = default)
    {
        var documents = new List<S>();

        using (var feedIterator = _container.GetItemQueryIterator<S>(queryDefinition, requestOptions: requestOptions))
        {
            while (feedIterator.HasMoreResults)
            {
                var response = await feedIterator.ReadNextAsync(cancellationToken);
                requestChargeAccumulator?.Add(response.RequestCharge);
                documents.AddRange(response);
            }
        }
        return documents;
    }

    public async Task<(IReadOnlyList<S> Items, string? ContinuationToken)> ExecuteQueryPageAsync<S>(
        QueryDefinition queryDefinition,
        int maxItemCount,
        string? continuationToken = null,
        QueryRequestOptions? requestOptions = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveRequestOptions = requestOptions ?? new QueryRequestOptions();
        effectiveRequestOptions.MaxItemCount = maxItemCount;

        using var feedIterator = _container.GetItemQueryIterator<S>(
            queryDefinition,
            continuationToken,
            effectiveRequestOptions);

        if (!feedIterator.HasMoreResults)
        {
            return ([], null);
        }

        var response = await feedIterator.ReadNextAsync(cancellationToken);
        return (response.ToList(), response.ContinuationToken);
    }

    public async Task<T> GetById(string id, PartitionKey partitionKey, CancellationToken cancellationToken = default)
    {
        return await _container.ReadItemAsync<T>(id, partitionKey, cancellationToken: cancellationToken);
    }

    public async Task<T?> GetByIdMaybe(string id, PartitionKey partitionKey, CancellationToken cancellationToken = default)
    {
        try
        {
            return await GetById(id, partitionKey, cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return default;
        }
    }

    public virtual async Task<IEnumerable<T>> GetByIdsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
    {
        const int MaxIdsPerQuery = 256;
        var allDocuments = new List<T>();

        var idChunks = ids
            .Select((id, index) => new { id, index })
            .GroupBy(x => x.index / MaxIdsPerQuery)
            .Select(group => group.Select(x => x.id).ToList());

        foreach (var chunk in idChunks)
        {
            var queryText = "SELECT * FROM c WHERE c.id IN (" +
                            string.Join(",", chunk.Select((_, i) => $"@id{i}")) + ")";

            var queryDefinition = new QueryDefinition(queryText);

            for (int i = 0; i < chunk.Count; i++)
            {
                queryDefinition.WithParameter($"@id{i}", chunk[i]);
            }

            var queryResult = await ExecuteQueryAsync<T>(queryDefinition, cancellationToken: cancellationToken);
            allDocuments.AddRange(queryResult);
        }

        return allDocuments
            .DistinctBy(document => document.Id, StringComparer.Ordinal);
    }

    public async Task<List<string>> GetAllIds(CancellationToken cancellationToken = default)
    {
        var sqlQuery = "SELECT VALUE c.id FROM c";

        List<string> ids = [];

        QueryDefinition queryDefinition = new(sqlQuery);
        using (FeedIterator<string> resultSet = _container.GetItemQueryIterator<string>(queryDefinition))
        {
            while (resultSet.HasMoreResults)
            {
                FeedResponse<string> response = await resultSet.ReadNextAsync(cancellationToken);
                ids.AddRange(response);
            }
        }
        return ids;
    }

    public async Task UpsertDocument(T document, CosmosWritePriority priority = CosmosWritePriority.Normal, CancellationToken cancellationToken = default)
    {
        await CosmosWriteThrottle.WaitForTurnAsync(priority, cancellationToken);
        try
        {
            await _container.UpsertItemAsync(document, cancellationToken: cancellationToken);
            // Delay is intentionally held inside the lock: releasing first would allow the
            // next waiter to start immediately, bypassing the intended write-rate cap.
            await Task.Delay(CosmosWriteThrottle.DelayBetweenWrites, cancellationToken);
        }
        finally
        {
            CosmosWriteThrottle.Semaphore.Release();
        }
    }
    public async Task BulkUpsert(
        IEnumerable<T> documents,
        int maxDegreeOfParallelism = 1,
        CosmosWritePriority priority = CosmosWritePriority.Normal,
        CancellationToken cancellationToken = default)
    {
        if (documents == null)
            return;

        var docs = documents.ToList();
        if (docs.Count == 0)
            return;

        if (maxDegreeOfParallelism <= 1)
        {
            foreach (var document in docs)
            {
                await CosmosWriteThrottle.WaitForTurnAsync(priority, cancellationToken);
                try
                {
                    await _container.UpsertItemAsync(document, cancellationToken: cancellationToken);
                    // Delay is intentionally held inside the lock: releasing first would allow the
                    // next waiter to start immediately, bypassing the intended write-rate cap.
                    await Task.Delay(CosmosWriteThrottle.DelayBetweenWrites, cancellationToken);
                }
                finally
                {
                    CosmosWriteThrottle.Semaphore.Release();
                }
            }
            return;
        }

        using var concurrencySemaphore = new SemaphoreSlim(maxDegreeOfParallelism);
        var tasks = docs.Select(async document =>
        {
            await concurrencySemaphore.WaitAsync(cancellationToken);
            try
            {
                await _container.UpsertItemAsync(document, cancellationToken: cancellationToken);
            }
            finally
            {
                concurrencySemaphore.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    public async Task DeleteDocument(string id, PartitionKey partitionKey, CancellationToken cancellationToken = default)
    {
        await _container.DeleteItemAsync<T>(id, partitionKey, cancellationToken: cancellationToken);
    }

    public async Task<IEnumerable<string>> GetIdsByKey(string key, string value, CancellationToken cancellationToken = default)
    {
        var validatedKey = ValidateQueryPropertyName(key);
        var queryDefinition = new QueryDefinition("SELECT VALUE c.id FROM c WHERE c." + validatedKey + " = @value")
            .WithParameter("@value", value);
        return await ExecuteQueryAsync<string>(queryDefinition, cancellationToken: cancellationToken);
    }

    public async Task DeleteDocumentsByKey(string key, string value, string? partitionKey = null, CancellationToken cancellationToken = default)
    {
        var references = await GetDocumentDeleteReferencesByKey(key, value, cancellationToken);
        var tasks = references.Select(reference => DeleteDocumentByCandidates(reference, value, partitionKey, cancellationToken));
        await Task.WhenAll(tasks);
    }

    public async Task ExpireDocumentsByKey(string key, string value, string? partitionKey = null, int ttlSeconds = 1, CancellationToken cancellationToken = default)
    {
        var references = await GetDocumentDeleteReferencesByKey(key, value, cancellationToken);
        var tasks = references.Select(reference => ExpireDocumentByCandidates(reference, value, partitionKey, ttlSeconds, cancellationToken));
        await Task.WhenAll(tasks);
    }

    private sealed class DocumentDeleteReference
    {
        public required string Id { get; init; }
        public string? KeyValue { get; init; }
    }

    private async Task<IEnumerable<DocumentDeleteReference>> GetDocumentDeleteReferencesByKey(string key, string value, CancellationToken cancellationToken)
    {
        var validatedKey = ValidateQueryPropertyName(key);
        var queryDefinition = new QueryDefinition($"SELECT c.id, c.{validatedKey} AS keyValue FROM c WHERE c.{validatedKey} = @value")
            .WithParameter("@value", value);

        return await ExecuteQueryAsync<DocumentDeleteReference>(queryDefinition, cancellationToken: cancellationToken);
    }

    private async Task DeleteDocumentByCandidates(
        DocumentDeleteReference reference,
        string queryValue,
        string? explicitPartitionKey,
        CancellationToken cancellationToken)
    {
        var candidateKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var candidate in new[] { explicitPartitionKey, reference.KeyValue, reference.Id, queryValue })
        {
            if (!string.IsNullOrWhiteSpace(candidate))
            {
                candidateKeys.Add(candidate);
            }
        }

        foreach (var candidate in candidateKeys)
        {
            try
            {
                await DeleteDocument(reference.Id, new PartitionKey(candidate), cancellationToken);
                return;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
            }
        }

        _logger.LogWarning(
            "Unable to delete document {DocumentId} in {DocumentType} by key-based lookup after trying {CandidateCount} partition key candidates. This may indicate a partition-key mismatch or that the document was already deleted.",
            reference.Id,
            typeof(T).Name,
            candidateKeys.Count);
    }

    private async Task ExpireDocumentByCandidates(
        DocumentDeleteReference reference,
        string queryValue,
        string? explicitPartitionKey,
        int ttlSeconds,
        CancellationToken cancellationToken)
    {
        var candidateKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var candidate in new[] { explicitPartitionKey, reference.KeyValue, reference.Id, queryValue })
        {
            if (!string.IsNullOrWhiteSpace(candidate))
            {
                candidateKeys.Add(candidate);
            }
        }

        foreach (var candidate in candidateKeys)
        {
            try
            {
                await PatchDocument(
                    reference.Id,
                    new PartitionKey(candidate),
                    [PatchOperation.Set("/ttl", ttlSeconds)],
                    cancellationToken: cancellationToken);
                return;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
            }
        }

        _logger.LogWarning(
            "Unable to mark document {DocumentId} in {DocumentType} for expiry after trying {CandidateCount} partition key candidates. This may indicate a partition-key mismatch, TTL not being enabled on the container, or that the document was already deleted.",
            reference.Id,
            typeof(T).Name,
            candidateKeys.Count);
    }

    private static string ValidateQueryPropertyName(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
            throw new ArgumentException("Query property name cannot be empty.", nameof(key));

        if (!char.IsLetter(key[0]) && key[0] != '_')
            throw new ArgumentException($"Invalid query property name '{key}'.", nameof(key));

        if (key.Any(ch => !char.IsLetterOrDigit(ch) && ch != '_'))
            throw new ArgumentException($"Invalid query property name '{key}'.", nameof(key));

        return key;
    }

    public async Task PatchDocument(
        string id,
        PartitionKey partitionKey,
        IReadOnlyList<PatchOperation> operations,
        string? ifMatchEtag = null,
        CosmosWritePriority priority = CosmosWritePriority.Normal,
        CancellationToken cancellationToken = default)
    {
        await CosmosWriteThrottle.WaitForTurnAsync(priority, cancellationToken);
        try
        {
            PatchItemRequestOptions? requestOptions = string.IsNullOrWhiteSpace(ifMatchEtag)
                ? null
                : new PatchItemRequestOptions { IfMatchEtag = ifMatchEtag };
            await _container.PatchItemAsync<T>(id, partitionKey, operations, requestOptions, cancellationToken);
            await Task.Delay(CosmosWriteThrottle.DelayBetweenWrites, cancellationToken);
        }
        finally
        {
            CosmosWriteThrottle.Semaphore.Release();
        }
    }

    public async Task PatchDocuments(
        IEnumerable<(string Id, PartitionKey PartitionKey, IReadOnlyList<PatchOperation> Operations)> patches,
        int maxDegreeOfParallelism = 1,
        CosmosWritePriority priority = CosmosWritePriority.Normal,
        CancellationToken cancellationToken = default)
    {
        var patchList = patches.ToList();
        if (patchList.Count == 0) return;

        if (maxDegreeOfParallelism <= 1)
        {
            await CosmosWriteThrottle.WaitForTurnAsync(priority, cancellationToken);
            try
            {
                foreach (var (id, partitionKey, operations) in patchList)
                {
                    await _container.PatchItemAsync<T>(id, partitionKey, operations, cancellationToken: cancellationToken);
                }
                await Task.Delay(CosmosWriteThrottle.DelayBetweenWrites, cancellationToken);
            }
            finally
            {
                CosmosWriteThrottle.Semaphore.Release();
            }
            return;
        }

        // Concurrent path: bypasses the global write throttle (same pattern as BulkUpsert).
        // Only suitable for dedicated backfill / admin operations that own the container's RU budget.
        using var concurrencySemaphore = new SemaphoreSlim(maxDegreeOfParallelism);
        var tasks = patchList.Select(async patch =>
        {
            await concurrencySemaphore.WaitAsync(cancellationToken);
            try
            {
                await _container.PatchItemAsync<T>(patch.Id, patch.PartitionKey, patch.Operations, cancellationToken: cancellationToken);
            }
            finally
            {
                concurrencySemaphore.Release();
            }
        });
        await Task.WhenAll(tasks);
    }


    private const int CosmosBatchMaxOperations = 100;

    public async Task ExecuteBatch(
        PartitionKey partitionKey,
        IEnumerable<T>? creates = null,
        IEnumerable<(string Id, IReadOnlyList<PatchOperation> Operations)>? patches = null,
        CancellationToken cancellationToken = default)
    {
        // Cosmos transactional batches are capped at 100 operations; chunk accordingly.
        var createsList = (creates ?? []).ToList();
        var patchesList = (patches ?? []).ToList();
        int total = createsList.Count + patchesList.Count;
        if (total == 0) return;

        int createIndex = 0;
        int patchIndex = 0;

        while (createIndex < createsList.Count || patchIndex < patchesList.Count)
        {
            var batch = _container.CreateTransactionalBatch(partitionKey);
            int count = 0;

            while (createIndex < createsList.Count && count < CosmosBatchMaxOperations)
            {
                batch.CreateItem(createsList[createIndex++]);
                count++;
            }
            while (patchIndex < patchesList.Count && count < CosmosBatchMaxOperations)
            {
                var (id, operations) = patchesList[patchIndex++];
                batch.PatchItem(id, operations);
                count++;
            }

            await CosmosWriteThrottle.Semaphore.WaitAsync(cancellationToken);
            try
            {
                using var response = await batch.ExecuteAsync(cancellationToken);
                if (!response.IsSuccessStatusCode)
                    throw new CosmosException(
                        $"Batch failed with status {response.StatusCode}",
                        response.StatusCode, 0, response.ActivityId, response.RequestCharge);
                await Task.Delay(CosmosWriteThrottle.DelayBetweenWrites, cancellationToken);
            }
            finally
            {
                CosmosWriteThrottle.Semaphore.Release();
            }
        }
    }
}
