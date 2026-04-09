using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class ProtectedAreasCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : ICollectionClient<StoredFeature>
{
    private readonly CollectionClient<StoredFeature> _collectionClient = new(container, loggerFactory);
    private readonly OverpassClient _overpassClient = overpassClient;
    private const int DefaultZoom = 8;

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = DefaultZoom)
    {
        var documentsById = new Dictionary<string, StoredFeature>();
        foreach (var (x, y) in keys.Distinct())
        {
            var tileDocuments = await FetchByTile(x, y, zoom);
            foreach (var document in tileDocuments)
            {
                if (!IsTileMarker(document))
                {
                    documentsById[document.Id] = document;
                }
            }
        }

        return documentsById.Values;
    }

    private async Task<IEnumerable<StoredFeature>> FetchByTile(int x, int y, int zoom)
    {
        var existingDocuments = (await QueryTile(x, y, zoom)).ToList();
        if (existingDocuments.Count != 0)
        {
            return existingDocuments.Where(doc => !IsTileMarker(doc));
        }

        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var fetchedFeatures = (await _overpassClient.GetProtectedAreas(southWest, northEast))
            .Select(feature => new StoredFeature(feature, zoom))
            .ToList();

        if (fetchedFeatures.Count == 0)
        {
            fetchedFeatures.Add(new StoredFeature(x, y, zoom));
        }
        await BulkUpsert(fetchedFeatures);
        return fetchedFeatures;
    }

    private async Task<IEnumerable<StoredFeature>> QueryTile(int x, int y, int zoom)
    {
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.X = @x AND c.Y = @y AND c.Zoom = @zoom"
        )
            .WithParameter("@x", x)
            .WithParameter("@y", y)
            .WithParameter("@zoom", zoom);
        return await ExecuteQueryAsync<StoredFeature>(query);
    }

    private static bool IsTileMarker(StoredFeature document)
    {
        return document.Id.StartsWith("empty-");
    }

    public Task<IEnumerable<StoredFeature>> FetchWholeCollection()
    {
        return _collectionClient.FetchWholeCollection();
    }

    public Task<IEnumerable<S>> ExecuteQueryAsync<S>(QueryDefinition queryDefinition, QueryRequestOptions? requestOptions = null)
    {
        return _collectionClient.ExecuteQueryAsync<S>(queryDefinition, requestOptions);
    }

    public Task<StoredFeature> GetById(string id, PartitionKey partitionKey)
    {
        return _collectionClient.GetById(id, partitionKey);
    }

    public Task<StoredFeature?> GetByIdMaybe(string id, PartitionKey partitionKey)
    {
        return _collectionClient.GetByIdMaybe(id, partitionKey);
    }

    public Task<IEnumerable<StoredFeature>> GetByIdsAsync(IEnumerable<string> ids)
    {
        return _collectionClient.GetByIdsAsync(ids);
    }

    public Task<List<string>> GetAllIds()
    {
        return _collectionClient.GetAllIds();
    }

    public Task UpsertDocument(StoredFeature document)
    {
        return _collectionClient.UpsertDocument(document);
    }

    public Task BulkUpsert(IEnumerable<StoredFeature> documents)
    {
        return _collectionClient.BulkUpsert(documents);
    }

    public Task DeleteDocument(string id, PartitionKey partitionKey)
    {
        return _collectionClient.DeleteDocument(id, partitionKey);
    }

    public Task<IEnumerable<string>> GetIdsByKey(string key, string value)
    {
        return _collectionClient.GetIdsByKey(key, value);
    }

    public Task DeleteDocumentsByKey(string key, string value, string? partitionKey = null)
    {
        return _collectionClient.DeleteDocumentsByKey(key, value, partitionKey);
    }
}