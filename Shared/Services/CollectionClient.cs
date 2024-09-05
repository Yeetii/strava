using Microsoft.Azure.Cosmos;
using Shared.Models;

namespace Shared.Services;

public class CollectionClient<T>(Container _container)
{
    public async Task<List<T>> GeoSpatialFetch(Coordinate center, int radius)
    {
        string query = string.Join(Environment.NewLine,
        "SELECT *",
        "FROM p",
        $"WHERE ST_DISTANCE(p.geometry, {{'type': 'Point', 'coordinates':[{center.Lng}, {center.Lat}]}}) < {radius}");

        return await QueryCollection(query);
    }

    public async Task<List<T>> FetchWholeCollection()
    {
        return await QueryCollection("SELECT * FROM p");
    }

    public async Task<IEnumerable<T>> QueryCollectionByXYIndex(int x, int y)
    {
        var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.x = @x  AND c.y = @y")
           .WithParameter("@x", x)
           .WithParameter("@y", y);

        var partitionKey = new PartitionKeyBuilder()
            .Add(x)
            .Add(y)
            .Build();

        var requestOptions = new QueryRequestOptions { PartitionKey = partitionKey };

        var iterator = _container.GetItemQueryIterator<T>(
            queryDefinition,
            requestOptions: requestOptions);

        List<T> results = [];

        while (iterator.HasMoreResults)
        {
            var response = await iterator.ReadNextAsync();
            results.AddRange(response);
        }
        return results;
    }

    public async Task<List<T>> QueryCollection(string query)
    {
        List<T> documents = [];

        QueryDefinition queryDefinition = new(query);
        using (FeedIterator<T> resultSet = _container.GetItemQueryIterator<T>(queryDefinition))
        {
            while (resultSet.HasMoreResults)
            {
                FeedResponse<T> response = await resultSet.ReadNextAsync();
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
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            return default;
        }
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
        const int maxConcurrentThreads = 50;
        var concurrentTasks = new List<Task>();
        var semaphore = new SemaphoreSlim(maxConcurrentThreads);

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
}