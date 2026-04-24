using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
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

    [Fact]
    public async Task FetchMissingTile_WithNoFetcher_ReturnsEmptyWithoutMarkers()
    {
        var client = new NoFetcherTiledCollectionClient(null!, new LoggerFactory(), FeatureKinds.Race, storeZoom: 8);
        var result = await client.FetchMissingTilePublic(10, 20, 8, CancellationToken.None);

        Assert.Empty(result);
    }

    private sealed class NoFetcherTiledCollectionClient : TiledCollectionClient
    {
        public NoFetcherTiledCollectionClient(Container container, ILoggerFactory loggerFactory, string kind, int storeZoom)
            : base(container, loggerFactory, kind, null, null, storeZoom)
        {
        }

        public Task<IEnumerable<StoredFeature>> FetchMissingTilePublic(int x, int y, int zoom, CancellationToken cancellationToken)
            => base.FetchMissingTile(x, y, zoom, cancellationToken);
    }
}
