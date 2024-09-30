using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Helpers;
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

    public async Task<IEnumerable<StoredFeature>> QueryByTwoPartitionKeys(int x, int y)
    {
        var queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.x = @x  AND c.y = @y")
           .WithParameter("@x", x)
           .WithParameter("@y", y);

        var partitionKey = new PartitionKeyBuilder()
            .Add(x)
            .Add(y)
            .Build();

        var requestOptions = new QueryRequestOptions { PartitionKey = partitionKey, };

        return await ExecuteQueryAsync<StoredFeature>(queryDefinition, requestOptions);
    }

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = 11)
    {
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
        var (nw, se) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);

        var rawPeaks = await _overpassClient.GetPeaks(nw, se);
        var peaks = RawPeakToStoredFeature(x, y, rawPeaks);
        if (!peaks.Any())
        {
            var emptyTileMarker = new StoredFeature
            {
                X = x,
                Y = y,
                Id = "empty-" + x + "-" + y,
                Geometry = new Geometry
                {
                    Type = "Point",
                    Coordinates = [0, 0]
                }
            };
            peaks = [emptyTileMarker];
        }
        await BulkUpsert(peaks);
        return peaks;
    }

    private static IEnumerable<StoredFeature> RawPeakToStoredFeature(int x, int y, IEnumerable<RawPeaks> rawPeaks)
    {
        foreach (var p in rawPeaks)
        {
            var propertiesDirty = new Dictionary<string, string?>(){
                    {"elevation", p.Tags.Elevation},
                    {"name", p.Tags.Name},
                    {"nameSapmi", p.Tags.NameSapmi},
                    {"nameAlt", p.Tags.NameAlt}
                };

            var properties = propertiesDirty.Where(x => x.Value != null).ToDictionary();

            yield return new StoredFeature
            {
                Id = p.Id.ToString(),
                X = x,
                Y = y,
                Properties = properties,
                Geometry = new Geometry { Coordinates = [p.Lon, p.Lat], Type = GeometryType.Point }
            };
        };
    }

    public Task<IEnumerable<StoredFeature>> FetchWholeCollection()
    {
        return _collectionClient.FetchWholeCollection();
    }

    public Task<IEnumerable<S>> ExecuteQueryAsync<S>(QueryDefinition queryDefinition, QueryRequestOptions requestOptions = null)
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