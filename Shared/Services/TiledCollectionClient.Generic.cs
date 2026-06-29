using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class TiledCollectionClient<TDocument>(
    Container container,
    ILoggerFactory loggerFactory,
    int storeZoom = 11)
    : CollectionClient<TDocument>(container, loggerFactory)
    where TDocument : TiledDocument
{
    protected readonly int _storeZoom = storeZoom;

    public Task<IEnumerable<TDocument>> FetchByTiles(
        IEnumerable<(int x, int y)> keys,
        int? zoom = null,
        CancellationToken cancellationToken = default)
    {
        return FetchByTilesCore(keys, zoom, null, cancellationToken);
    }

    public async Task<MeasuredResult<IEnumerable<TDocument>>> FetchByTilesMeasured(
        IEnumerable<(int x, int y)> keys,
        int? zoom = null,
        CancellationToken cancellationToken = default)
    {
        var requestChargeAccumulator = new RequestChargeAccumulator();
        var features = await FetchByTilesCore(keys, zoom, requestChargeAccumulator, cancellationToken);
        return new MeasuredResult<IEnumerable<TDocument>>(features, requestChargeAccumulator.TotalRequestCharge);
    }

    protected async Task<IEnumerable<TDocument>> FetchByTilesCore(
        IEnumerable<(int x, int y)> keys,
        int? zoom,
        RequestChargeAccumulator? requestChargeAccumulator,
        CancellationToken cancellationToken)
    {
        if (!keys.Any())
            return [];

        var requestedZoom = zoom ?? _storeZoom;
        var requestedKeys = keys.Distinct().ToList();
        return await FetchByTilesCore(requestedKeys, requestedZoom, requestChargeAccumulator, cancellationToken);
    }

    protected virtual async Task<IEnumerable<TDocument>> FetchByTilesCore(
        IReadOnlyList<(int x, int y)> requestedKeys,
        int requestedZoom,
        RequestChargeAccumulator? requestChargeAccumulator,
        CancellationToken cancellationToken)
    {
        var storageKeys = GetStorageKeysForRequestedTiles(requestedKeys, requestedZoom);

        var canonicalDocs = (await QueryCanonicalDocuments(storageKeys, requestChargeAccumulator, cancellationToken)).ToList();
        canonicalDocs.AddRange(await FetchMissingDocuments(canonicalDocs, storageKeys, requestChargeAccumulator, cancellationToken));

        var exactRequestedDocs = requestedZoom != _storeZoom
            ? await QueryExactRequestedDocuments(requestedKeys, requestedZoom, requestChargeAccumulator, cancellationToken)
            : Enumerable.Empty<TDocument>();

        var docs = requestedZoom != _storeZoom
            ? FilterToRequestedTiles(canonicalDocs, requestedKeys, requestedZoom).Concat(exactRequestedDocs)
            : canonicalDocs;

        return FinalizeFetchedDocuments(docs);
    }

    protected virtual async Task<IEnumerable<TDocument>> QueryCanonicalDocuments(
        IEnumerable<(int x, int y)> storageKeys,
        RequestChargeAccumulator? requestChargeAccumulator,
        CancellationToken cancellationToken)
        => await QueryByListOfKeys(storageKeys, _storeZoom, requestChargeAccumulator, cancellationToken);

    protected virtual async Task<IEnumerable<TDocument>> QueryExactRequestedDocuments(
        IReadOnlyList<(int x, int y)> requestedKeys,
        int requestedZoom,
        RequestChargeAccumulator? requestChargeAccumulator,
        CancellationToken cancellationToken)
        => await QueryByListOfKeys(requestedKeys, requestedZoom, requestChargeAccumulator, cancellationToken);

    protected virtual async Task<IEnumerable<TDocument>> FetchMissingDocuments(
        IEnumerable<TDocument> canonicalDocs,
        IEnumerable<(int x, int y)> storageKeys,
        RequestChargeAccumulator? requestChargeAccumulator,
        CancellationToken cancellationToken)
    {
        var missingStorageTiles = GetMissingTiles(canonicalDocs.Select(d => (d.X, d.Y)), storageKeys);
        var fetched = new List<TDocument>();
        foreach (var (x, y) in missingStorageTiles)
            fetched.AddRange(await FetchMissingTile(x, y, _storeZoom, cancellationToken));
        return fetched;
    }

    protected virtual IEnumerable<TDocument> FinalizeFetchedDocuments(IEnumerable<TDocument> documents)
        => documents.DistinctBy(d => d.Id, StringComparer.Ordinal);

    protected virtual async Task<IEnumerable<TDocument>> QueryByListOfKeys(
        IEnumerable<(int x, int y)> keys,
        int zoom,
        RequestChargeAccumulator? requestChargeAccumulator = null,
        CancellationToken cancellationToken = default)
    {
        var keyList = keys.Distinct().ToList();
        if (keyList.Count == 0)
            return [];

        const int MaxKeysPerTileQuery = 100;
        if (keyList.Count > MaxKeysPerTileQuery)
        {
            var allResults = new List<TDocument>();
            foreach (var batch in keyList.Chunk(MaxKeysPerTileQuery))
                allResults.AddRange(await QueryByListOfKeys(batch, zoom, requestChargeAccumulator, cancellationToken));
            return allResults;
        }

        var keyConditions = string.Join(" OR ", keyList.Select((_, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryDefinition = new QueryDefinition($"SELECT * FROM c WHERE ({keyConditions}) AND c.zoom = @zoom")
            .WithParameter("@zoom", zoom);

        for (var i = 0; i < keyList.Count; i++)
        {
            var (x, y) = keyList[i];
            queryDefinition = queryDefinition
                .WithParameter($"@x{i}", x)
                .WithParameter($"@y{i}", y);
        }

        return await ExecuteQueryAsync<TDocument>(queryDefinition, null, requestChargeAccumulator, cancellationToken);
    }

    protected virtual Task<IEnumerable<TDocument>> FetchMissingTile(int x, int y, int zoom, CancellationToken cancellationToken = default)
        => Task.FromResult<IEnumerable<TDocument>>([]);

    protected static IEnumerable<(int x, int y)> GetMissingTiles(IEnumerable<(int x, int y)> presentKeys, IEnumerable<(int x, int y)> keys)
    {
        var keysInDocuments = presentKeys.ToHashSet();
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
            for (var offsetY = 0; offsetY < scale; offsetY++)
                yield return (baseX + offsetX, baseY + offsetY);
    }

    internal (int x, int y) ConvertToParentStorageKey((int x, int y) requestedKey, int requestedZoom)
    {
        var zoomDelta = requestedZoom - _storeZoom;
        if (zoomDelta <= 0)
            return requestedKey;

        var scale = 1 << zoomDelta;
        return (requestedKey.x / scale, requestedKey.y / scale);
    }

    internal static IEnumerable<TDocument> FilterToRequestedTiles(IEnumerable<TDocument> storedDocuments, IEnumerable<(int x, int y)> requestedKeys, int requestedZoom)
    {
        var requestedTileSet = new HashSet<(int x, int y)>(requestedKeys);
        return storedDocuments.Where(document => requestedTileSet.Contains(SlippyTileCalculator.WGS84ToTileIndex(document.ResolvedCentroid, requestedZoom)));
    }
}
