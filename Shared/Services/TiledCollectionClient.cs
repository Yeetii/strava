using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class TiledCollectionClient(
    Container container,
    ILoggerFactory loggerFactory,
    string kind,
    Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> fetchFromOverpass,
    Func<IEnumerable<string>, CancellationToken, Task<IEnumerable<Feature>>>? fetchByIds = null)
    : CollectionClient<StoredFeature>(container, loggerFactory)
{
    protected readonly string _kind = kind;
    private readonly Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> _fetchFromOverpass = fetchFromOverpass;
    private readonly Func<IEnumerable<string>, CancellationToken, Task<IEnumerable<Feature>>>? _fetchByIds = fetchByIds;

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(
        IEnumerable<(int x, int y)> keys,
        int zoom = 11,
        bool followPointers = false,
        CancellationToken cancellationToken = default)
    {
        if (!keys.Any())
            return [];

        var docs = (await QueryByListOfKeys(keys, zoom, cancellationToken)).ToList();
        var missingTiles = GetMissingTiles(docs, keys);
        foreach (var (x, y) in missingTiles)
            docs.AddRange(await FetchMissingTile(x, y, zoom, cancellationToken));

        var visibleDocuments = docs
            .Where(d => !d.Id.StartsWith("empty-"))
            .ToList();

        if (followPointers)
            visibleDocuments = (await ResolvePointers(visibleDocuments, cancellationToken)).ToList();

        return visibleDocuments
            .OrderBy(d => StoredFeature.IsPointerDocument(d))
            .DistinctBy(d => d.LogicalId);
    }

    protected virtual async Task<IEnumerable<StoredFeature>> QueryByListOfKeys(IEnumerable<(int x, int y)> keys, int zoom, CancellationToken cancellationToken = default)
    {
        var keyConditions = string.Join(" OR ", keys.Select((_, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryDefinition = new QueryDefinition($"SELECT * FROM c WHERE ({keyConditions}) AND c.zoom = @zoom AND c.kind = @kind")
            .WithParameter("@zoom", zoom)
            .WithParameter("@kind", _kind);
        int index = 0;
        foreach (var (x, y) in keys)
        {
            queryDefinition = queryDefinition
                .WithParameter($"@x{index}", x)
                .WithParameter($"@y{index}", y);
            index++;
        }
        return await ExecuteQueryAsync<StoredFeature>(queryDefinition, cancellationToken: cancellationToken);
    }

    protected static IEnumerable<(int x, int y)> GetMissingTiles(IEnumerable<StoredFeature> documents, IEnumerable<(int x, int y)> keys)
    {
        var keysInDocuments = new HashSet<(int x, int y)>(documents.Select(d => (d.X, d.Y)));
        return keys.Where(k => !keysInDocuments.Contains((k.x, k.y)));
    }

    public async Task<IEnumerable<StoredFeature>> GetByFeatureIdsAsync(IEnumerable<string> featureIds, CancellationToken cancellationToken = default)
    {
        var normalizedIds = featureIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Select(id => StoredFeature.NormalizeFeatureId(_kind, id))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var ids = normalizedIds
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .SelectMany(id => new[]
            {
                id,
                StoredFeature.EnsurePrefixedFeatureId(_kind, id)
            })
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var storedFeatures = (await GetByIdsAsync(ids, cancellationToken)).ToList();
        if (_fetchByIds == null)
            return storedFeatures;

        var fetchedIds = storedFeatures
            .Select(feature => feature.LogicalId)
            .ToHashSet(StringComparer.Ordinal);
        var missingIds = normalizedIds
            .Where(id => !fetchedIds.Contains(id))
            .ToArray();

        if (missingIds.Length == 0)
            return storedFeatures;

        var missingFeatures = (await _fetchByIds(missingIds, cancellationToken))
            .Select(feature => new StoredFeature(feature, _kind))
            .ToList();

        if (missingFeatures.Count == 0)
            return storedFeatures;

        await BulkUpsert(missingFeatures, cancellationToken);
        storedFeatures.AddRange(missingFeatures);
        return storedFeatures;
    }

    public async Task<IEnumerable<StoredFeature>> GeoSpatialFetch(Coordinate center, int radius, CancellationToken cancellationToken = default)
    {
        var query = new QueryDefinition(
            "SELECT * FROM p WHERE p.kind = @kind AND ST_DISTANCE(p.geometry, {'type': 'Point', 'coordinates':[@lng, @lat]}) < @radius")
            .WithParameter("@kind", _kind)
            .WithParameter("@lng", center.Lng)
            .WithParameter("@lat", center.Lat)
            .WithParameter("@radius", radius);
        return await ExecuteQueryAsync<StoredFeature>(query, cancellationToken: cancellationToken);
    }

    protected virtual async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom, CancellationToken cancellationToken = default)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var rawFeatures = await _fetchFromOverpass(southWest, northEast, cancellationToken);

        var features = rawFeatures
            .SelectMany(f => CreateDocumentsForTile(f, x, y, zoom))
            .ToList();

        if (features.Count == 0)
            features.Add(new StoredFeature(_kind, x, y, zoom));

        await BulkUpsert(features, cancellationToken);
        return features;
    }

    protected virtual IEnumerable<StoredFeature> CreateDocumentsForTile(Feature feature, int requestedX, int requestedY, int zoom)
    {
        var storedFeature = new StoredFeature(feature, _kind, zoom);
        yield return storedFeature;

        if (feature.Geometry is Point || (storedFeature.X == requestedX && storedFeature.Y == requestedY))
            yield break;

        yield return StoredFeature.CreatePointer(
            _kind,
            storedFeature.FeatureId ?? storedFeature.LogicalId,
            requestedX,
            requestedY,
            zoom,
            storedFeature.X,
            storedFeature.Y,
            storedFeature.Zoom,
            storedFeature.Id);
    }

    private async Task<IEnumerable<StoredFeature>> ResolvePointers(IEnumerable<StoredFeature> documents, CancellationToken cancellationToken)
    {
        var documentList = documents.ToList();
        var pointedIds = documentList
            .Where(StoredFeature.IsPointerDocument)
            .Select(StoredFeature.GetPointerStoredDocumentId)
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (pointedIds.Count == 0)
            return documentList;

        var resolvedDocuments = (await GetByIdsAsync(pointedIds!, cancellationToken))
            .ToDictionary(document => document.Id, StringComparer.Ordinal);

        return documentList
            .Select(document =>
            {
                if (!StoredFeature.IsPointerDocument(document))
                    return document;

                var storedDocumentId = StoredFeature.GetPointerStoredDocumentId(document);
                if (storedDocumentId != null && resolvedDocuments.TryGetValue(storedDocumentId, out var resolved))
                    return resolved;

                return document;
            })
            .Where(document => !StoredFeature.IsPointerDocument(document));
    }
}
