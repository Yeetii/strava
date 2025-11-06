using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Geo;
using Microsoft.Extensions.Logging;

namespace Shared.Services;

public class PeaksCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient) : ICollectionClient<StoredFeature>
{
    private readonly CollectionClient<StoredFeature> _collectionClient = new(container, loggerFactory);
    private readonly OverpassClient _overpassClient = overpassClient;

    public async Task<IEnumerable<StoredFeature>> GeoSpatialFetch(Coordinate center, int radius)
    {
        string query = string.Join(Environment.NewLine,
        "SELECT *",
        "FROM p",
        $"WHERE ST_DISTANCE(p.geometry, {{'type': 'Point', 'coordinates':[{center.Lng}, {center.Lat}]}}) < {radius}");

        return await ExecuteQueryAsync<StoredFeature>(new QueryDefinition(query));
    }

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = 11)
    {
        if (!keys.Any())
        {
            return [];
        }
        var peaks = await QueryByListOfKeys(keys);
        var missingTiles = GetMissingTiles(peaks, keys);
        foreach (var (x, y) in missingTiles)
        {
            var freshPeaks = await FetchMissingTile(x, y, zoom);
            peaks = peaks.Concat(freshPeaks);
        }
        return peaks.Where(p => !p.Id.StartsWith("empty"));
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

    private static IEnumerable<(int x, int y)> GetMissingTiles(IEnumerable<StoredFeature> peaks, IEnumerable<(int x, int y)> keys)
    {
        var keysInPeaks = new HashSet<(int x, int y)>(peaks.Select(p => (p.X, p.Y)));
        var missingTiles = keys.Where(p => !keysInPeaks.Contains((p.x, p.y)));
        return missingTiles;
    }

    private async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom)
    {
        var (sw, ne) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);

        var rawPeaks = await _overpassClient.GetPeaks(sw, ne);
        var peaks = rawPeaks.Select(x => new StoredFeature(x));
        if (!peaks.Any())
        {
            var emptyTileMarker = new StoredFeature(x, y);
            peaks = [emptyTileMarker];
        }
        await BulkUpsert(peaks);
        return peaks;
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