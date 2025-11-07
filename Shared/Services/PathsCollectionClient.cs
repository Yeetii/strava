using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Geo;
using Microsoft.Extensions.Logging;
using BAMCIS.GeoJSON;

namespace Shared.Services;

public class PathsCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient) : ICollectionClient<StoredFeature>
{
    private readonly CollectionClient<StoredFeature> _collectionClient = new(container, loggerFactory);
    private readonly OverpassClient _overpassClient = overpassClient;

    public async Task<FeatureCollection> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = 11)
    {
        if (!keys.Any())
            return new FeatureCollection(features: []);
        
        var paths = (await QueryByListOfKeys(keys)).ToList();
        var missingTiles = GetMissingTiles(paths, keys);

        foreach (var (x, y) in missingTiles)
            paths.AddRange(await FetchMissingTile(x, y, zoom));

        var features = paths
            .Where(p => !p.Id.StartsWith("empty"))
            .Select(p => p.ToFeature());

        return new FeatureCollection(features);
    }

    private async Task<IEnumerable<StoredFeature>> QueryByListOfKeys(IEnumerable<(int x, int y)> keys)
    {
        var keyConditions = string.Join(" OR ", keys.Select((key, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryDefinition = new QueryDefinition($"SELECT * FROM c WHERE {keyConditions}");
        int index = 0;
        foreach (var (x, y) in keys)
        {
            queryDefinition = queryDefinition
                .WithParameter($"@x{index}", x)
                .WithParameter($"@y{index}", y);
            index++;
        }
        return await ExecuteQueryAsync<StoredFeature>(queryDefinition);
    }

    private static IEnumerable<(int x, int y)> GetMissingTiles(IEnumerable<StoredFeature> paths, IEnumerable<(int x, int y)> keys)
    {
        var keysInPaths = new HashSet<(int x, int y)>(paths.Select(p => (p.X, p.Y)));
        var missingTiles = keys.Where(p => !keysInPaths.Contains((p.x, p.y)));
        return missingTiles;
    }

    private async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom)
    {
        var (sw, ne) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var rawPaths = await _overpassClient.GetPaths(sw, ne);
        var paths = rawPaths.Select(x => new StoredFeature(x));
        if (!paths.Any())
        {
            var emptyTileMarker = new StoredFeature(x, y);
            paths = [emptyTileMarker];
        }
        await BulkUpsert(paths);
        return paths;
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
