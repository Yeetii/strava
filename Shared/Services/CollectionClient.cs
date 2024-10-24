using Microsoft.Azure.Cosmos;
using Shared.Models;
using System.Net;
using Microsoft.Extensions.Logging;

namespace Shared.Services;

public class CollectionClient<T>(Container _container, ILoggerFactory loggerFactory) where T : IDocument
{
    private readonly ILogger<CollectionClient<T>> _logger = loggerFactory.CreateLogger<CollectionClient<T>>();
    const int maxConcurrentThreads = 2;
    private readonly SemaphoreSlim semaphore = new(maxConcurrentThreads);

    public async Task<IEnumerable<T>> FetchWholeCollection()
    {
        return await ExecuteQueryAsync<T>(new QueryDefinition("SELECT * FROM p"));
    }

    public async Task<IEnumerable<S>> ExecuteQueryAsync<S>(QueryDefinition queryDefinition, QueryRequestOptions requestOptions = null)
    {
        var documents = new List<S>();

        using (var feedIterator = _container.GetItemQueryIterator<S>(queryDefinition, requestOptions: requestOptions))
        {
            while (feedIterator.HasMoreResults)
            {
                var response = await feedIterator.ReadNextAsync();
                documents.AddRange(response);
            }
        }
        return documents;
    }

    public async Task<T> GetById(string id, PartitionKey partitionKey)
    {
        return await _container.ReadItemAsync<T>(id, partitionKey);
    }

    public async Task<T?> GetByIdMaybe(string id, PartitionKey partitionKey)
    {
        try
        {
            return await GetById(id, partitionKey);
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return default;
        }
    }

    public async Task<IEnumerable<T>> GetByIdsAsync(IEnumerable<string> ids)
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

            var queryResult = await ExecuteQueryAsync<T>(queryDefinition);
            allDocuments.AddRange(queryResult);
        }

        return allDocuments;
    }

    public async Task<List<string>> GetAllIds()
    {
        var sqlQuery = "SELECT VALUE c.id FROM c";

        List<string> ids = [];

        QueryDefinition queryDefinition = new(sqlQuery);
        using (FeedIterator<string> resultSet = _container.GetItemQueryIterator<string>(queryDefinition))
        {
            while (resultSet.HasMoreResults)
            {
                FeedResponse<string> response = await resultSet.ReadNextAsync();
                ids.AddRange(response);
            }
        }
        return ids;
    }

    public async Task UpsertDocument(T document)
    {
        await _container.UpsertItemAsync(document);
    }
    public async Task BulkUpsert(IEnumerable<T> documents)
    {
        var concurrentTasks = new List<Task>();

        foreach (var document in documents)
        {
            await semaphore.WaitAsync();
            concurrentTasks.Add(Task.Run(async () =>
            {
                try
                {
                    await _container.UpsertItemAsync(document);
                }
                finally
                {
                    semaphore.Release();
                }
            }));
        }
        await Task.WhenAll(concurrentTasks);
    }

    public async Task DeleteDocument(string id, PartitionKey partitionKey)
    {
        await _container.DeleteItemAsync<T>(id, partitionKey);
    }

    public async Task<IEnumerable<string>> GetIdsByKey(string key, string value)
    {
        var queryDefinition = new QueryDefinition("SELECT VALUE c.id FROM c WHERE c." + key + " = @value")
            .WithParameter("@value", value);
        return await ExecuteQueryAsync<string>(queryDefinition);
    }

    public async Task DeleteDocumentsByKey(string key, string value, string? partitionKey = null)
    {
        var ids = await GetIdsByKey(key, value);
        var tasks = ids.Select(id => DeleteDocument(id, new PartitionKey(partitionKey ?? id)));
        await Task.WhenAll(tasks);
    }
}