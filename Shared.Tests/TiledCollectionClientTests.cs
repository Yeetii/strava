using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class TiledCollectionClientTests
{
    [Fact]
    public void GetStorageKeysForRequestedTiles_WhenRequestedZoomIsSmallerThanStoreZoom_ExpandsToStorageZoom()
    {
        var client = CreateClient(storeZoom: 11);
        var requestedKeys = new[] { (10, 20) };

        var storageKeys = client.GetStorageKeysForRequestedTiles(requestedKeys, 10)
            .OrderBy(key => key.x)
            .ThenBy(key => key.y)
            .ToList();

        Assert.Equal(new[] { (20, 40), (20, 41), (21, 40), (21, 41) }, storageKeys);
    }

    [Fact]
    public void GetStorageKeysForRequestedTiles_WhenRequestedZoomIsLargerThanStoreZoom_ReducesToParentTile()
    {
        var client = CreateClient(storeZoom: 11);
        var requestedKeys = new[] { (20, 40), (21, 41) };

        var storageKeys = client.GetStorageKeysForRequestedTiles(requestedKeys, 12)
            .OrderBy(key => key.x)
            .ThenBy(key => key.y)
            .ToList();

        Assert.Equal(new[] { (10, 20) }, storageKeys);
    }

    private static TiledCollectionClient CreateClient(int storeZoom)
    {
        static Task<IEnumerable<Feature>> fetchFromOverpass(Coordinate _c1, Coordinate _c2, CancellationToken _)
            => Task.FromResult(Enumerable.Empty<Feature>());

        return new TiledCollectionClient(
            null!,
            new LoggerFactory(),
            FeatureKinds.Peak,
fetchFromOverpass,
            null,
            storeZoom);
    }
}
