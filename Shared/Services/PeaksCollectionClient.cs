using Microsoft.Azure.Cosmos;
using Shared.Models;
using Shared.Geo;
using Microsoft.Extensions.Logging;

namespace Shared.Services;

public class PeaksCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : TiledCollectionClient(container, loggerFactory, overpassClient)
{
    public async Task<IEnumerable<StoredFeature>> GeoSpatialFetch(Coordinate center, int radius, CancellationToken cancellationToken = default)
    {
        string query = string.Join(Environment.NewLine,
        "SELECT *",
        "FROM p",
        $"WHERE ST_DISTANCE(p.geometry, {{'type': 'Point', 'coordinates':[{center.Lng}, {center.Lat}]}}) < {radius}");

        return await ExecuteQueryAsync<StoredFeature>(new QueryDefinition(query), cancellationToken: cancellationToken);
    }

    protected override async Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom, CancellationToken cancellationToken = default)
    {
        var (sw, ne) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var peaks = (await _overpassClient.GetPeaks(sw, ne, cancellationToken)).Select(f => new StoredFeature(f, zoom));
        if (!peaks.Any())
            peaks = [new StoredFeature(x, y, zoom)];
        await BulkUpsert(peaks, cancellationToken);
        return peaks;
    }
}
