using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class ProtectedAreasCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : TiledCollectionClient(container, loggerFactory, overpassClient)
{
    public new async Task<IEnumerable<StoredFeature>> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = 8)
    {
        var areas = await base.FetchByTiles(keys, zoom);
        return areas.DistinctBy(area => area.LogicalId);
    }

    protected override async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var features = (await _overpassClient.GetProtectedAreas(southWest, northEast))
            .Select(feature => new StoredFeature(feature, x, y, zoom, storePerTile: true))
            .ToList();

        if (features.Count == 0)
            features.Add(new StoredFeature(x, y, zoom));

        await BulkUpsert(features);
        return features;
    }
}
