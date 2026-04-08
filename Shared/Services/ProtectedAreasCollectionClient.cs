using System.Globalization;
using BAMCIS.GeoJSON;
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
        if (existingDocuments.Any(document => document.Id == GetTileMarkerId(x, y)))
        {
            return existingDocuments;
        }

        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var fetchedFeatures = (await _overpassClient.GetProtectedAreas(southWest, northEast))
            .Select(feature => new StoredFeature(feature))
            .ToList();

        fetchedFeatures.Add(CreateTileMarker(x, y, southWest, northEast));
        await BulkUpsert(fetchedFeatures);
        return fetchedFeatures;
    }

    private async Task<IEnumerable<StoredFeature>> QueryTile(int x, int y, int zoom)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var polygon = CreateTilePolygonLiteral(southWest, northEast);
        var query = new QueryDefinition($"SELECT * FROM c WHERE ST_INTERSECTS(c.geometry, {polygon})");
        return await ExecuteQueryAsync<StoredFeature>(query);
    }

    private static string CreateTilePolygonLiteral(Coordinate southWest, Coordinate northEast)
    {
        var west = southWest.Lng.ToString(CultureInfo.InvariantCulture);
        var south = southWest.Lat.ToString(CultureInfo.InvariantCulture);
        var east = northEast.Lng.ToString(CultureInfo.InvariantCulture);
        var north = northEast.Lat.ToString(CultureInfo.InvariantCulture);

        return $"{{\"type\":\"Polygon\",\"coordinates\":[[[{west},{south}],[{east},{south}],[{east},{north}],[{west},{north}],[{west},{south}]]]}}";
    }

    private static StoredFeature CreateTileMarker(int x, int y, Coordinate southWest, Coordinate northEast)
    {
        var center = new Position(
            (southWest.Lng + northEast.Lng) / 2,
            (southWest.Lat + northEast.Lat) / 2
        );
        return new StoredFeature
        {
            Id = GetTileMarkerId(x, y),
            X = x,
            Y = y,
            Geometry = new Point(center, null),
            Properties = new Dictionary<string, dynamic>
            {
                ["tileMarker"] = true,
            },
        };
    }

    private static string GetTileMarkerId(int x, int y)
    {
        return $"protected-area-tile-{x}-{y}";
    }

    private static bool IsTileMarker(StoredFeature document)
    {
        return document.Id.StartsWith("protected-area-tile-");
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