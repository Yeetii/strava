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
}

public class CollectionClient<T>(Container _container, ILoggerFactory loggerFactory) where T : IDocument
{
    private readonly ILogger<CollectionClient<T>> _logger = loggerFactory.CreateLogger<CollectionClient<T>>();

    public async Task<IEnumerable<T>> FetchWholeCollection(CancellationToken cancellationToken = default)
    {
        return await ExecuteQueryAsync<T>(new QueryDefinition("SELECT * FROM p"), cancellationToken: cancellationToken);
    }

    public async Task<IEnumerable<S>> ExecuteQueryAsync<S>(QueryDefinition queryDefinition, QueryRequestOptions? requestOptions = null, CancellationToken cancellationToken = default)
    {
        var documents = new List<S>();

        using (var feedIterator = _container.GetItemQueryIterator<S>(queryDefinition, requestOptions: requestOptions))
        {
            while (feedIterator.HasMoreResults)
            {
                var response = await feedIterator.ReadNextAsync(cancellationToken);
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

    public async Task<IEnumerable<T>> GetByIdsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
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

        return allDocuments;
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

    public async Task UpsertDocument(T document, CancellationToken cancellationToken = default)
    {
        await CosmosWriteThrottle.Semaphore.WaitAsync(cancellationToken);
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
    public async Task BulkUpsert(IEnumerable<T> documents, CancellationToken cancellationToken = default)
    {
        foreach (var document in documents)
        {
            await CosmosWriteThrottle.Semaphore.WaitAsync(cancellationToken);
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
    }

    public async Task DeleteDocument(string id, PartitionKey partitionKey, CancellationToken cancellationToken = default)
    {
        await _container.DeleteItemAsync<T>(id, partitionKey, cancellationToken: cancellationToken);
    }

    public async Task<IEnumerable<string>> GetIdsByKey(string key, string value, CancellationToken cancellationToken = default)
    {
        var queryDefinition = new QueryDefinition("SELECT VALUE c.id FROM c WHERE c." + key + " = @value")
            .WithParameter("@value", value);
        return await ExecuteQueryAsync<string>(queryDefinition, cancellationToken: cancellationToken);
    }

    public async Task DeleteDocumentsByKey(string key, string value, string? partitionKey = null, CancellationToken cancellationToken = default)
    {
        var ids = await GetIdsByKey(key, value, cancellationToken);
        var tasks = ids.Select(id => DeleteDocument(id, new PartitionKey(partitionKey ?? id), cancellationToken));
        await Task.WhenAll(tasks);
    }
}