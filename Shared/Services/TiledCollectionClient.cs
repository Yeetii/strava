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
    Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>>? fetcher = null,
    Func<IEnumerable<string>, CancellationToken, Task<IEnumerable<Feature>>>? fetchByIds = null,
    int storeZoom = 11)
    : CollectionClient<StoredFeature>(container, loggerFactory)
{
    protected readonly string _kind = kind;
    private readonly Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<Feature>>>? _fetcher = fetcher;
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

    // Cosmos SQL predicate that matches the GeoJSON point passed in via @lng/@lat
    // when the document geometry is a polygon/multi-polygon that contains it.
    private const string WithinPredicate =
        "ST_WITHIN({'type':'Point','coordinates':[@lng,@lat]}, c.geometry)";
    private const int MaxPointsPerContainmentQuery = 25;
    private const string StoredFeatureSummaryProjection = "c.id, c.featureId, c.kind, c.properties";

    // Cosmos SQL predicate that matches documents whose geometry is within @radius
    // metres of the GeoJSON point passed in via @lng/@lat.
    private const string WithinRadiusPredicate =
        "ST_DISTANCE(c.geometry, {'type':'Point','coordinates':[@lng,@lat]}) <= @radius";

    // Excludes pointer and empty-marker docs so the spatial predicate only runs against
    // real feature geometry. Cheap to evaluate before the ST_* call.
    private const string RealDocumentPredicate =
        "(NOT IS_DEFINED(c.properties.isPointer) OR c.properties.isPointer = false) " +
        "AND NOT STARTSWITH(c.id, \"empty-\")";

    /// <summary>
    /// Returns the stored features whose geometry contains <paramref name="point"/>.
    /// Filters by the client's <c>kind</c> and by the slippy tile containing the point
    /// (partition-scoped) before running Cosmos' native <c>ST_WITHIN</c> — so big
    /// polygon documents are only materialised when they are actually candidates.
    /// Pointer documents that land on the tile are resolved so multi-tile features
    /// (e.g. countries) are still found.
    /// </summary>
    public Task<IEnumerable<StoredFeature>> FindFeaturesContainingPoint(
        Coordinate point,
        CancellationToken cancellationToken = default)
        => FindFeaturesContainingPoint(point, null, null, cancellationToken);

    public async Task<IEnumerable<StoredFeature>> FindFeaturesContainingPoint(
        Coordinate point,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken = default)
    {
        var tile = SlippyTileCalculator.WGS84ToTileIndex(point, _storeZoom);
        var spatialParameters = WithinParameters(point);

        var matches = await QueryAtTileWithSpatialPredicate<StoredFeature>(
            tile, "*", WithinPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        var pointerStoredIds = await GetPointerStoredIdsAtTile(tile, additionalFilter, additionalParameters, cancellationToken);
        var resolved = await ResolveStoredIdsWithSpatialPredicate<StoredFeature>(
            pointerStoredIds, "*", WithinPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        return matches
            .Concat(resolved)
            .DistinctBy(d => d.Id, StringComparer.Ordinal);
    }

    /// <summary>
    /// Same as <see cref="FindFeaturesContainingPoint(Coordinate, CancellationToken)"/> but projects only
    /// the document id. Useful when the caller just needs existence / identity and wants to avoid
    /// transferring large polygon geometries.
    /// </summary>
    public Task<IEnumerable<string>> FindFeatureIdsContainingPoint(
        Coordinate point,
        CancellationToken cancellationToken = default)
        => FindFeatureIdsContainingPoint(point, null, null, cancellationToken);

    public async Task<IEnumerable<string>> FindFeatureIdsContainingPoint(
        Coordinate point,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken = default)
    {
        var tile = SlippyTileCalculator.WGS84ToTileIndex(point, _storeZoom);
        var spatialParameters = WithinParameters(point);

        var matchIds = await QueryAtTileWithSpatialPredicate<string>(
            tile, "VALUE c.id", WithinPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        var pointerStoredIds = await GetPointerStoredIdsAtTile(tile, additionalFilter, additionalParameters, cancellationToken);
        var resolvedIds = await ResolveStoredIdsWithSpatialPredicate<string>(
            pointerStoredIds, "VALUE c.id", WithinPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        return matchIds.Concat(resolvedIds).Distinct(StringComparer.Ordinal);
    }

    /// <summary>
    /// Returns lightweight feature metadata for documents containing any of the supplied points.
    /// Points are grouped by storage tile and queried in small batches so Cosmos can use the
    /// partition key/tile pre-filter before evaluating the geospatial predicates.
    /// </summary>
    public Task<IEnumerable<StoredFeatureSummary>> FindFeatureSummariesContainingAnyPoint(
        IEnumerable<Coordinate> points,
        CancellationToken cancellationToken = default)
        => FindFeatureSummariesContainingAnyPoint(points, null, null, cancellationToken);

    public async Task<IEnumerable<StoredFeatureSummary>> FindFeatureSummariesContainingAnyPoint(
        IEnumerable<Coordinate> points,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken = default)
    {
        var pointsByTile = points
            .Select(point => new { Point = point, Tile = SlippyTileCalculator.WGS84ToTileIndex(point, _storeZoom) })
            .DistinctBy(item => (item.Tile, item.Point.Lng, item.Point.Lat))
            .GroupBy(item => item.Tile, item => item.Point)
            .ToList();

        if (pointsByTile.Count == 0)
            return [];

        var results = new List<StoredFeatureSummary>();
        foreach (var tileGroup in pointsByTile)
        {
            var pointerStoredIds = (await GetPointerStoredIdsAtTile(
                tileGroup.Key, additionalFilter, additionalParameters, cancellationToken)).ToList();

            foreach (var pointBatch in tileGroup.Chunk(MaxPointsPerContainmentQuery))
            {
                var spatialPredicate = BuildWithinAnyPointPredicate(pointBatch);
                var spatialParameters = WithinAnyPointParameters(pointBatch);

                var matches = await QueryAtTileWithSpatialPredicate<StoredFeatureSummary>(
                    tileGroup.Key,
                    StoredFeatureSummaryProjection,
                    spatialPredicate,
                    spatialParameters,
                    additionalFilter,
                    additionalParameters,
                    cancellationToken);
                results.AddRange(matches);

                var resolved = await ResolveStoredIdsWithSpatialPredicate<StoredFeatureSummary>(
                    pointerStoredIds,
                    StoredFeatureSummaryProjection,
                    spatialPredicate,
                    spatialParameters,
                    additionalFilter,
                    additionalParameters,
                    cancellationToken);
                results.AddRange(resolved);
            }
        }

        return results.DistinctBy(summary => summary.Id, StringComparer.Ordinal);
    }

    /// <summary>
    /// Returns the stored features within <paramref name="radiusMeters"/> of <paramref name="point"/>.
    /// Filters by the client's <c>kind</c> and by the set of slippy tiles covering the
    /// bounding box around the radius, then runs Cosmos' native <c>ST_DISTANCE</c>.
    /// Pointer documents in any candidate tile are resolved so multi-tile features still match.
    /// </summary>
    public Task<IEnumerable<StoredFeature>> FindFeaturesWithinRadius(
        Coordinate point,
        int radiusMeters,
        CancellationToken cancellationToken = default)
        => FindFeaturesWithinRadius(point, radiusMeters, null, null, cancellationToken);

    public async Task<IEnumerable<StoredFeature>> FindFeaturesWithinRadius(
        Coordinate point,
        int radiusMeters,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken = default)
    {
        var tiles = GetTilesCoveringRadius(point, radiusMeters);
        var spatialParameters = WithinRadiusParameters(point, radiusMeters);

        var matches = await QueryAtTilesWithSpatialPredicate<StoredFeature>(
            tiles, "*", WithinRadiusPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        var pointerStoredIds = await GetPointerStoredIdsAtTiles(tiles, additionalFilter, additionalParameters, cancellationToken);
        var resolved = await ResolveStoredIdsWithSpatialPredicate<StoredFeature>(
            pointerStoredIds, "*", WithinRadiusPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        return matches
            .Concat(resolved)
            .DistinctBy(d => d.Id, StringComparer.Ordinal);
    }

    /// <summary>
    /// Same as <see cref="FindFeaturesWithinRadius(Coordinate, int, CancellationToken)"/> but projects only
    /// the document id to save RU when large geometries would otherwise be transferred.
    /// </summary>
    public Task<IEnumerable<string>> FindFeatureIdsWithinRadius(
        Coordinate point,
        int radiusMeters,
        CancellationToken cancellationToken = default)
        => FindFeatureIdsWithinRadius(point, radiusMeters, null, null, cancellationToken);

    public async Task<IEnumerable<string>> FindFeatureIdsWithinRadius(
        Coordinate point,
        int radiusMeters,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken = default)
    {
        var tiles = GetTilesCoveringRadius(point, radiusMeters);
        var spatialParameters = WithinRadiusParameters(point, radiusMeters);

        var matchIds = await QueryAtTilesWithSpatialPredicate<string>(
            tiles, "VALUE c.id", WithinRadiusPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        var pointerStoredIds = await GetPointerStoredIdsAtTiles(tiles, additionalFilter, additionalParameters, cancellationToken);
        var resolvedIds = await ResolveStoredIdsWithSpatialPredicate<string>(
            pointerStoredIds, "VALUE c.id", WithinRadiusPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        return matchIds.Concat(resolvedIds).Distinct(StringComparer.Ordinal);
    }

    /// <summary>
    /// Returns the set of slippy-tile keys at <see cref="_storeZoom"/> whose bounding boxes
    /// cover the axis-aligned square of <paramref name="radiusMeters"/> around
    /// <paramref name="center"/>. This is the cheap bounding-box pre-filter required by the
    /// Geo-First Performance principle before running <c>ST_DISTANCE</c>.
    /// </summary>
    internal IReadOnlyList<(int x, int y)> GetTilesCoveringRadius(Coordinate center, int radiusMeters)
    {
        var southWest = GeoSpatialFunctions.ShiftCoordinate(center, -radiusMeters, -radiusMeters);
        var northEast = GeoSpatialFunctions.ShiftCoordinate(center, radiusMeters, radiusMeters);

        var swTile = SlippyTileCalculator.WGS84ToTileIndex(southWest, _storeZoom);
        var neTile = SlippyTileCalculator.WGS84ToTileIndex(northEast, _storeZoom);

        // Slippy tile Y grows southwards; normalise so we can iterate with min/max.
        var minX = Math.Min(swTile.x, neTile.x);
        var maxX = Math.Max(swTile.x, neTile.x);
        var minY = Math.Min(swTile.y, neTile.y);
        var maxY = Math.Max(swTile.y, neTile.y);

        var tiles = new List<(int x, int y)>((maxX - minX + 1) * (maxY - minY + 1));
        for (var x = minX; x <= maxX; x++)
            for (var y = minY; y <= maxY; y++)
                tiles.Add((x, y));
        return tiles;
    }

    private static Dictionary<string, object?> WithinParameters(Coordinate point) => new()
    {
        ["@lng"] = point.Lng,
        ["@lat"] = point.Lat,
    };

    private static Dictionary<string, object?> WithinRadiusParameters(Coordinate point, int radiusMeters) => new()
    {
        ["@lng"] = point.Lng,
        ["@lat"] = point.Lat,
        ["@radius"] = radiusMeters,
    };

    private static string BuildWithinAnyPointPredicate(IReadOnlyList<Coordinate> points)
    {
        if (points.Count == 1)
            return "ST_WITHIN({'type':'Point','coordinates':[@lng0,@lat0]}, c.geometry)";

        return "(" + string.Join(" OR ", points.Select((_, index) =>
            $"ST_WITHIN({{'type':'Point','coordinates':[@lng{index},@lat{index}]}}, c.geometry)")) + ")";
    }

    private static Dictionary<string, object?> WithinAnyPointParameters(IReadOnlyList<Coordinate> points)
    {
        var parameters = new Dictionary<string, object?>(points.Count * 2);
        for (var index = 0; index < points.Count; index++)
        {
            parameters[$"@lng{index}"] = points[index].Lng;
            parameters[$"@lat{index}"] = points[index].Lat;
        }

        return parameters;
    }

    private async Task<IEnumerable<T>> QueryAtTileWithSpatialPredicate<T>(
        (int x, int y) tile,
        string projection,
        string spatialPredicate,
        IReadOnlyDictionary<string, object?> spatialParameters,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken)
    {
        var queryText = $"SELECT {projection} FROM c " +
                        "WHERE c.x = @x AND c.y = @y AND c.kind = @kind " +
                        $"AND {RealDocumentPredicate} " +
                        $"AND {spatialPredicate}";
        if (!string.IsNullOrWhiteSpace(additionalFilter))
            queryText += $" AND ({additionalFilter})";

        var queryDefinition = new QueryDefinition(queryText)
            .WithParameter("@kind", _kind)
            .WithParameter("@x", tile.x)
            .WithParameter("@y", tile.y);
        ApplyParameters(queryDefinition, spatialParameters);
        ApplyParameters(queryDefinition, additionalParameters);

        var requestOptions = new QueryRequestOptions
        {
            PartitionKey = new PartitionKeyBuilder().Add((double)tile.x).Add((double)tile.y).Build()
        };
        return await ExecuteQueryAsync<T>(queryDefinition, requestOptions, cancellationToken);
    }

    private async Task<IEnumerable<T>> QueryAtTilesWithSpatialPredicate<T>(
        IReadOnlyList<(int x, int y)> tiles,
        string projection,
        string spatialPredicate,
        IReadOnlyDictionary<string, object?> spatialParameters,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken)
    {
        if (tiles.Count == 0)
            return [];

        if (tiles.Count == 1)
            return await QueryAtTileWithSpatialPredicate<T>(
                tiles[0], projection, spatialPredicate, spatialParameters, additionalFilter, additionalParameters, cancellationToken);

        var keyConditions = string.Join(" OR ", tiles.Select((_, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryText = $"SELECT {projection} FROM c " +
                        $"WHERE ({keyConditions}) AND c.kind = @kind " +
                        $"AND {RealDocumentPredicate} " +
                        $"AND {spatialPredicate}";
        if (!string.IsNullOrWhiteSpace(additionalFilter))
            queryText += $" AND ({additionalFilter})";

        var queryDefinition = new QueryDefinition(queryText)
            .WithParameter("@kind", _kind);
        for (var i = 0; i < tiles.Count; i++)
        {
            queryDefinition = queryDefinition
                .WithParameter($"@x{i}", tiles[i].x)
                .WithParameter($"@y{i}", tiles[i].y);
        }
        ApplyParameters(queryDefinition, spatialParameters);
        ApplyParameters(queryDefinition, additionalParameters);

        return await ExecuteQueryAsync<T>(queryDefinition, cancellationToken: cancellationToken);
    }

    private Task<IEnumerable<string>> GetPointerStoredIdsAtTile(
        (int x, int y) tile,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken)
        => GetPointerStoredIdsAtTiles(new[] { tile }, additionalFilter, additionalParameters, cancellationToken);

    private async Task<IEnumerable<string>> GetPointerStoredIdsAtTiles(
        IReadOnlyList<(int x, int y)> tiles,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken)
    {
        if (tiles.Count == 0)
            return [];

        var keyConditions = string.Join(" OR ", tiles.Select((_, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryText = $"SELECT VALUE c.properties.{StoredFeature.PointerStoredDocumentIdProperty} FROM c " +
                        $"WHERE ({keyConditions}) AND c.kind = @kind " +
                        "AND c.properties.isPointer = true";
        if (!string.IsNullOrWhiteSpace(additionalFilter))
            queryText += $" AND ({additionalFilter})";

        var queryDefinition = new QueryDefinition(queryText).WithParameter("@kind", _kind);
        for (var i = 0; i < tiles.Count; i++)
        {
            queryDefinition = queryDefinition
                .WithParameter($"@x{i}", tiles[i].x)
                .WithParameter($"@y{i}", tiles[i].y);
        }
        ApplyParameters(queryDefinition, additionalParameters);

        QueryRequestOptions? requestOptions = null;
        if (tiles.Count == 1)
        {
            requestOptions = new QueryRequestOptions
            {
                PartitionKey = new PartitionKeyBuilder().Add((double)tiles[0].x).Add((double)tiles[0].y).Build()
            };
        }

        var ids = await ExecuteQueryAsync<string>(queryDefinition, requestOptions, cancellationToken);
        return ids
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal);
    }

    private async Task<IEnumerable<T>> ResolveStoredIdsWithSpatialPredicate<T>(
        IEnumerable<string> storedIds,
        string projection,
        string spatialPredicate,
        IReadOnlyDictionary<string, object?> spatialParameters,
        string? additionalFilter,
        IReadOnlyDictionary<string, object?>? additionalParameters,
        CancellationToken cancellationToken)
    {
        var idList = storedIds.ToList();
        if (idList.Count == 0)
            return [];

        // Match GetByIdsAsync chunking to stay under the Cosmos SQL IN(...) limit.
        const int maxIdsPerQuery = 256;
        var results = new List<T>();
        for (var offset = 0; offset < idList.Count; offset += maxIdsPerQuery)
        {
            var chunk = idList.Skip(offset).Take(maxIdsPerQuery).ToList();
            var idParams = string.Join(",", chunk.Select((_, i) => $"@id{i}"));
            var queryText = $"SELECT {projection} FROM c " +
                            $"WHERE c.id IN ({idParams}) AND c.kind = @kind " +
                            $"AND {RealDocumentPredicate} " +
                            $"AND {spatialPredicate}";
            if (!string.IsNullOrWhiteSpace(additionalFilter))
                queryText += $" AND ({additionalFilter})";

            var queryDefinition = new QueryDefinition(queryText).WithParameter("@kind", _kind);
            for (var i = 0; i < chunk.Count; i++)
                queryDefinition = queryDefinition.WithParameter($"@id{i}", chunk[i]);
            ApplyParameters(queryDefinition, spatialParameters);
            ApplyParameters(queryDefinition, additionalParameters);

            results.AddRange(await ExecuteQueryAsync<T>(queryDefinition, cancellationToken: cancellationToken));
        }
        return results;
    }

    private static void ApplyParameters(QueryDefinition queryDefinition, IReadOnlyDictionary<string, object?>? parameters)
    {
        if (parameters is null)
            return;
        foreach (var (name, value) in parameters)
            queryDefinition.WithParameter(name, value);
    }

    protected virtual async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom, CancellationToken cancellationToken = default)
    {
        if (_fetcher is null)
            return [];

        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var rawFeatures = await _fetcher(southWest, northEast, cancellationToken);

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
            .GroupBy(document => document.Id, StringComparer.Ordinal)
            .Select(group => group.First())
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
