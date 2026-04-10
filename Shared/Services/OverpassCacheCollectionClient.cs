using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services;

public class OverpassCacheCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : CollectionClient<OverpassCacheDocument>(container, loggerFactory)
{
    private readonly OverpassClient _overpassClient = overpassClient;

    /// <summary>
    /// Fetches features for the given tile from the Cosmos cache, or queries Overpass if not cached.
    /// </summary>
    /// <param name="x">Tile X index.</param>
    /// <param name="y">Tile Y index.</param>
    /// <param name="zoom">Zoom level.</param>
    /// <param name="query">
    /// Full Overpass QL query with <c>{{bbox}}</c> as a placeholder for the bounding box
    /// (format: minLat,minLon,maxLat,maxLon).
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<FeatureCollection> FetchByTile(int x, int y, int zoom, string query, CancellationToken cancellationToken = default)
    {
        var queryHash = ComputeQueryHash(query);
        var id = OverpassCacheDocument.MakeId(queryHash, zoom, x, y);
        var partitionKey = OverpassCacheDocument.MakePartitionKey(x, y);

        var cached = await GetByIdMaybe(id, new PartitionKey(partitionKey), cancellationToken)
                  ?? await GetByIdMaybe($"empty-{id}", new PartitionKey(partitionKey), cancellationToken);

        if (cached != null)
        {
            if (cached.Id.StartsWith("empty-"))
                return new FeatureCollection([]);

            var cachedFeatures = JsonSerializer.Deserialize<List<Feature>>(cached.FeaturesJson) ?? [];
            return new FeatureCollection(cachedFeatures);
        }

        // Not in cache — fetch from Overpass
        var (sw, ne) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var bbox = CreateBoundingBox(sw, ne);
        var boundedQuery = query.Replace("{{bbox}}", bbox);

        var features = (await _overpassClient.ExecuteGenericQuery(boundedQuery, cancellationToken)).ToList();

        var doc = features.Count > 0
            ? new OverpassCacheDocument
            {
                Id = id,
                PartitionKey = partitionKey,
                X = x,
                Y = y,
                Zoom = zoom,
                QueryHash = queryHash,
                FeaturesJson = JsonSerializer.Serialize(features),
            }
            : new OverpassCacheDocument
            {
                Id = $"empty-{id}",
                PartitionKey = partitionKey,
                X = x,
                Y = y,
                Zoom = zoom,
                QueryHash = queryHash,
                FeaturesJson = "[]",
            };

        await UpsertDocument(doc, cancellationToken);

        return new FeatureCollection(features);
    }

    private static string ComputeQueryHash(string query)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(query));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private static string CreateBoundingBox(Coordinate southWest, Coordinate northEast)
    {
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{southWest.Lat},{southWest.Lng},{northEast.Lat},{northEast.Lng}"
        );
    }
}
