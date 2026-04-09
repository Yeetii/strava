using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class ProtectedAreasCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : TiledCollectionClient(container, loggerFactory, overpassClient)
{
    public new async Task<IEnumerable<StoredFeature>> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = 8, CancellationToken cancellationToken = default)
    {
        var areas = await base.FetchByTiles(keys, zoom, cancellationToken);
        return areas.DistinctBy(area => area.LogicalId);
    }

    protected override async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom, CancellationToken cancellationToken = default)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var features = (await _overpassClient.GetProtectedAreas(southWest, northEast, cancellationToken))
            .Select(feature => new StoredFeature(feature, x, y, zoom, storePerTile: true))
            .ToList();

        if (features.Count == 0)
            features.Add(new StoredFeature(x, y, zoom));

        await BulkUpsert(features, cancellationToken);
        return features;
    }
}
