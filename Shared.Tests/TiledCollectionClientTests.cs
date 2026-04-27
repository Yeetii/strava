using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Geo;
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
    public void GetTilesCoveringRadius_WithTinyRadius_ReturnsOnlyCenterTile()
    {
        var client = CreateClient(storeZoom: 11);
        var point = new Coordinate(13.09363, 63.39677);
        var expected = SlippyTileCalculator.WGS84ToTileIndex(point, 11);

        var tiles = client.GetTilesCoveringRadius(point, 1).ToList();

        Assert.Single(tiles);
        Assert.Equal(expected, tiles[0]);
    }

    [Fact]
    public void GetTilesCoveringRadius_WithLargeRadius_ReturnsCenterTileAndNeighbours()
    {
        var client = CreateClient(storeZoom: 11);
        var point = new Coordinate(13.09363, 63.39677);
        var center = SlippyTileCalculator.WGS84ToTileIndex(point, 11);

        var tiles = client.GetTilesCoveringRadius(point, 50_000).ToList();

        Assert.Contains(center, tiles);
        Assert.True(tiles.Count > 1);
        Assert.All(tiles, t =>
        {
            Assert.InRange(t.x, center.x - 10, center.x + 10);
            Assert.InRange(t.y, center.y - 10, center.y + 10);
        });
        Assert.Equal(tiles.Count, tiles.Distinct().Count());
    }

    [Fact]
    public void GetTilesCoveringRadius_ScalesWithStoreZoom()
    {
        var point = new Coordinate(13.09363, 63.39677);

        var lowZoom = CreateClient(storeZoom: 6).GetTilesCoveringRadius(point, 50_000).Count;
        var highZoom = CreateClient(storeZoom: 11).GetTilesCoveringRadius(point, 50_000).Count;

        Assert.True(highZoom >= lowZoom,
            $"A deeper storeZoom must cover at least as many tiles (z11={highZoom}, z6={lowZoom})");
    }

    [Fact]
    public async Task FetchMissingTile_WithNoFetcher_ReturnsEmptyWithoutMarkers()
    {
        var client = new NoFetcherTiledCollectionClient(null!, new LoggerFactory(), FeatureKinds.Race, storeZoom: 8);
        var result = await client.FetchMissingTilePublic(10, 20, 8, CancellationToken.None);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ResolvePointers_WhenResolvedDocumentsHaveDuplicateIds_DeduplicatesBeforeDictionaryLookup()
    {
        const string kind = FeatureKinds.Peak;
        const string featureId = "duplicate-feature";
        const string storedDocumentId = "peak:duplicate-feature";

        var client = new DuplicateResolvePointersClient(null!, new LoggerFactory(), kind, storeZoom: 11);
        var pointer = StoredFeature.CreatePointer(
            kind,
            featureId,
            x: 1,
            y: 1,
            zoom: 11,
            storedX: 1,
            storedY: 1,
            storedZoom: 11,
            storedDocumentId);

        var result = (await client.ResolvePointersPublic(new[] { pointer }, CancellationToken.None)).ToList();

        Assert.Single(result);
        Assert.Equal(storedDocumentId, result[0].Id);
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

    private sealed class DuplicateResolvePointersClient : TiledCollectionClient
    {
        public DuplicateResolvePointersClient(Container container, ILoggerFactory loggerFactory, string kind, int storeZoom)
            : base(container, loggerFactory, kind, (_, _, cancellationToken) => Task.FromResult(Enumerable.Empty<Feature>()), null, storeZoom)
        {
        }

        public Task<IEnumerable<StoredFeature>> ResolvePointersPublic(IEnumerable<StoredFeature> documents, CancellationToken cancellationToken)
            => base.ResolvePointers(documents, cancellationToken);

        public override Task<IEnumerable<StoredFeature>> GetByIdsAsync(IEnumerable<string> ids, CancellationToken cancellationToken = default)
        {
            var id = ids.Single();
            var duplicate = new StoredFeature
            {
                Id = id,
                FeatureId = id,
                Kind = FeatureKinds.Peak,
                X = 0,
                Y = 0,
                Zoom = 11,
                Geometry = new Point(new Position(0, 0)),
                Properties = new Dictionary<string, dynamic>()
            };

            return Task.FromResult((IEnumerable<StoredFeature>)new[] { duplicate, duplicate });
        }
    }
}
