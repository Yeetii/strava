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
    Func<IEnumerable<string>, CancellationToken, Task<IEnumerable<Feature>>>? fetchByIds = null,
    int storeZoom = 11)
    : CollectionClient<StoredFeature>(container, loggerFactory)
{
    protected readonly string _kind = kind;
    private readonly Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>> _fetchFromOverpass = fetchFromOverpass;
    private readonly Func<IEnumerable<string>, CancellationToken, Task<IEnumerable<Feature>>>? _fetchByIds = fetchByIds;
    protected readonly int _storeZoom = storeZoom;

    public Task<IEnumerable<StoredFeature>> FetchByTiles(
        IEnumerable<(int x, int y)> keys,
        int? zoom = null,
        bool followPointers = false,
        CancellationToken cancellationToken = default)
    {
        return FetchByTiles(keys, null, null, zoom, followPointers, cancellationToken);
    }

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(
        IEnumerable<(int x, int y)> keys,
        string? filter,
        IReadOnlyDictionary<string, object?>? filterParameters,
        int? zoom = null,
        bool followPointers = false,
        CancellationToken cancellationToken = default)
    {
        if (!keys.Any())
            return [];

        var requestedZoom = zoom ?? _storeZoom;
        var requestedKeys = keys
            .Distinct()
            .ToList();

        var storageKeys = GetStorageKeysForRequestedTiles(requestedKeys, requestedZoom);
        var canonicalDocs = (await QueryByListOfKeys(storageKeys, _storeZoom, filter, filterParameters, cancellationToken)).ToList();
        var exactRequestedDocs = requestedZoom != _storeZoom
            ? [.. await QueryByListOfKeys(requestedKeys, requestedZoom, filter, filterParameters, cancellationToken)]
            : Enumerable.Empty<StoredFeature>();

        var missingStorageTiles = GetMissingTiles(canonicalDocs, storageKeys);
        foreach (var (x, y) in missingStorageTiles)
            canonicalDocs.AddRange(await FetchMissingTile(x, y, _storeZoom, cancellationToken));

        var docs = canonicalDocs;
        if (requestedZoom != _storeZoom)
        {
            docs = [.. FilterToRequestedTiles(canonicalDocs, requestedKeys, requestedZoom)
                , .. exactRequestedDocs];
        }

        var visibleDocuments = docs
            .Where(d => !d.Id.StartsWith("empty-"))
            .ToList();

        if (followPointers)
            visibleDocuments = (await ResolvePointers(visibleDocuments, cancellationToken)).ToList();

        return visibleDocuments
            .OrderBy(d => StoredFeature.IsPointerDocument(d))
            .DistinctBy(d => d.LogicalId);
    }

    protected virtual async Task<IEnumerable<StoredFeature>> QueryByListOfKeys(
        IEnumerable<(int x, int y)> keys,
        int zoom,
        string? additionalFilter = null,
        IReadOnlyDictionary<string, object?>? additionalParameters = null,
        CancellationToken cancellationToken = default)
    {
        var keyList = keys.Distinct().ToList();
        if (!keyList.Any())
            return [];

        var keyConditions = string.Join(" OR ", keyList.Select((_, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryText = $"SELECT * FROM c WHERE ({keyConditions}) AND c.zoom = @zoom AND c.kind = @kind";
        if (!string.IsNullOrWhiteSpace(additionalFilter))
            queryText += $" AND ({additionalFilter})";

        var queryDefinition = new QueryDefinition(queryText)
            .WithParameter("@zoom", zoom)
            .WithParameter("@kind", _kind);

        if (additionalParameters != null)
        {
            foreach (var (name, value) in additionalParameters)
            {
                queryDefinition = queryDefinition.WithParameter(name, value);
            }
        }

        int index = 0;
        foreach (var (x, y) in keyList)
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

    internal IEnumerable<(int x, int y)> GetStorageKeysForRequestedTiles(IEnumerable<(int x, int y)> requestedKeys, int requestedZoom)
    {
        if (requestedZoom == _storeZoom)
            return requestedKeys.Distinct();

        if (requestedZoom < _storeZoom)
            return requestedKeys.SelectMany(requestedKey => ExpandToStorageZoomKeys(requestedKey, requestedZoom)).Distinct();

        return requestedKeys
            .Select(requestedKey => ConvertToParentStorageKey(requestedKey, requestedZoom))
            .Distinct();
    }

    internal IEnumerable<(int x, int y)> ExpandToStorageZoomKeys((int x, int y) requestedKey, int requestedZoom)
    {
        var zoomDelta = _storeZoom - requestedZoom;
        var scale = 1 << zoomDelta;
        var baseX = requestedKey.x * scale;
        var baseY = requestedKey.y * scale;

        for (var offsetX = 0; offsetX < scale; offsetX++)
        {
            for (var offsetY = 0; offsetY < scale; offsetY++)
            {
                yield return (baseX + offsetX, baseY + offsetY);
            }
        }
    }

    internal (int x, int y) ConvertToParentStorageKey((int x, int y) requestedKey, int requestedZoom)
    {
        var zoomDelta = requestedZoom - _storeZoom;
        if (zoomDelta <= 0)
            return requestedKey;

        var scale = 1 << zoomDelta;
        return (requestedKey.x / scale, requestedKey.y / scale);
    }

    internal static IEnumerable<StoredFeature> FilterToRequestedTiles(IEnumerable<StoredFeature> storedDocuments, IEnumerable<(int x, int y)> requestedKeys, int requestedZoom)
    {
        var requestedTileSet = new HashSet<(int x, int y)>(requestedKeys);

        return storedDocuments.Where(document =>
        {
            var centroid = GeometryCentroidHelper.GetCentroid(document.Geometry);
            var tile = SlippyTileCalculator.WGS84ToTileIndex(centroid, requestedZoom);
            return requestedTileSet.Contains(tile);
        });
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
            .Select(feature => new StoredFeature(feature, _kind, _storeZoom))
            .ToList();

        if (missingFeatures.Count == 0)
            return storedFeatures;

        await BulkUpsert(missingFeatures, cancellationToken: cancellationToken);
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
            .SelectMany(f => CreateDocumentsForTile(f, x, y, zoom, zoom))
            .ToList();

        if (features.Count == 0)
            features.Add(new StoredFeature(_kind, x, y, zoom));

        await BulkUpsert(features, cancellationToken: cancellationToken);
        return features;
    }

    protected virtual IEnumerable<StoredFeature> CreateDocumentsForTile(Feature feature, int requestedX, int requestedY, int requestedZoom, int storedZoom)
    {
        var storedFeature = new StoredFeature(feature, _kind, storedZoom);
        yield return storedFeature;

        if (requestedZoom != storedZoom || feature.Geometry is Point || (storedFeature.X == requestedX && storedFeature.Y == requestedY))
            yield break;

        yield return StoredFeature.CreatePointer(
            _kind,
            storedFeature.FeatureId ?? storedFeature.LogicalId,
            requestedX,
            requestedY,
            requestedZoom,
            storedFeature.X,
            storedFeature.Y,
            storedFeature.Zoom,
            storedFeature.Id);
    }

    protected virtual async Task<IEnumerable<StoredFeature>> ResolvePointers(IEnumerable<StoredFeature> documents, CancellationToken cancellationToken)
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
