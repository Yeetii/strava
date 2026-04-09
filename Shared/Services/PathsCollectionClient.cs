using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Geo;
using Microsoft.Extensions.Logging;
using BAMCIS.GeoJSON;

namespace Shared.Services;

public class PathsCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : TiledCollectionClient(container, loggerFactory, overpassClient)
{
    public new async Task<FeatureCollection> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = 11, CancellationToken cancellationToken = default)
    {
        var features = (await base.FetchByTiles(keys, zoom, cancellationToken))
            .DistinctBy(feature => feature.LogicalId);
        return new FeatureCollection(features.Select(f => f.ToFeature()));
    }

    protected override async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom, CancellationToken cancellationToken = default)
    {
        var (sw, ne) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var paths = (await _overpassClient.GetPaths(sw, ne, cancellationToken))
            .Select(feature => new StoredFeature(feature, x, y, zoom, storePerTile: true));
        if (!paths.Any())
            paths = [new StoredFeature(x, y, zoom)];
        await BulkUpsert(paths, cancellationToken);
        return paths;
    }
}
