using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Admin;
using Shared.Services.Postgres.Spatial;
using Shared.Services.Postgres.Spatial.Ingestion;

namespace API.Endpoints.Admin;

public class FetchCountries(
    IOsmFeatureIngestionService ingestionService,
    IOsmFeatureAdminQueries adminQueries,
    IOsmTileQueryRepository tileQueries,
    IConfiguration configuration,
    ILogger<FetchCountries> logger)
{
    private const int CountryAdminLevel = 2;

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "countryCodes", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Comma-separated ISO 3166-1 alpha-2 codes. Resolved against already-stored level-2 boundaries; an unstored country cannot be resolved to a bbox.")]
    [OpenApiParameter(name: "osmIds", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Comma-separated OSM IDs (e.g. relation:2145268). Resolved against already-stored level-2 boundaries.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json",
        bodyType: typeof(CountryFetchResult))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(FetchCountries))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/countries/fetch")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var osmIdsParam = req.Query["osmIds"];
        var codesParam = req.Query["countryCodes"];

        IReadOnlyList<string>? countryCodes = null;
        IReadOnlyList<(string osmType, long osmId)>? osmIds = null;

        if (!string.IsNullOrWhiteSpace(osmIdsParam))
        {
            osmIds = osmIdsParam
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(id =>
                {
                    var parts = id.Split(':', 2);
                    return (osmType: parts[0], osmId: long.Parse(parts[1]));
                })
                .Distinct()
                .ToList();
        }
        else if (!string.IsNullOrWhiteSpace(codesParam))
        {
            countryCodes = codesParam
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .Select(c => c.ToUpperInvariant())
                .ToList();
        }

        var hasFilters = osmIds is { Count: > 0 } || countryCodes is { Count: > 0 };
        var requestedCount = osmIds?.Count ?? countryCodes?.Count ?? 0;

        // EnsureAdminBoundariesAsync is bbox-driven: codes/osm ids can only be mapped through
        // the geometry of an already-stored level-2 boundary. New countries need a discovery
        // (Overpass by ISO code / by id) step before they can be ingested.
        var areas = await adminQueries.ResolveAdminBoundaryAreasAsync(countryCodes, osmIds, cancellationToken);
        var tiles = new HashSet<(int x, int y)>();
        foreach (var area in areas)
        {
            foreach (var tile in ExpandAreaTiles(area, AdminOsmConstants.AdminBoundaryStorageZoom))
                tiles.Add(tile);
        }

        var unmapped = hasFilters ? Math.Max(0, requestedCount - areas.Count) : 0;
        if (unmapped > 0)
        {
            logger.LogWarning(
                "{Unmapped} of {Requested} requested country codes/OSM ids could not be mapped to stored boundaries; bbox-based ingestion cannot fetch countries that are not already stored",
                unmapped,
                requestedCount);
        }

        if (hasFilters)
        {
            // Explicit requests are force-refetches: clear the tile-query markers first.
            foreach (var (x, y) in tiles)
            {
                await tileQueries.Delete(
                    FeatureKinds.AdminBoundary,
                    AdminOsmConstants.AdminBoundaryStorageZoom,
                    x,
                    y,
                    cancellationToken);
            }
        }

        var points = tiles
            .Select(tile => TileCenter(tile.x, tile.y, AdminOsmConstants.AdminBoundaryStorageZoom))
            .ToList();

        var ingestion = points.Count > 0
            ? await ingestionService.EnsureAdminBoundariesAsync(points, CountryAdminLevel, cancellationToken)
            : OsmIngestionResult.Empty;

        var result = new CountryFetchResult(
            Requested: requestedCount,
            Resolved: areas.Count,
            FeaturesUpserted: ingestion.FeaturesUpserted,
            TilesQueried: ingestion.TilesQueried,
            TilesFailed: ingestion.TilesFailed,
            Unmapped: unmapped);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(result);
        return response;
    }

    private static IEnumerable<(int x, int y)> ExpandAreaTiles(AdminBoundaryArea area, int zoom)
    {
        var (swX, swY) = SlippyTileCalculator.WGS84ToTileIndex(new Coordinate(area.MinLng, area.MinLat), zoom);
        var (neX, neY) = SlippyTileCalculator.WGS84ToTileIndex(new Coordinate(area.MaxLng, area.MaxLat), zoom);

        var minX = Math.Min(swX, neX);
        var maxX = Math.Max(swX, neX);
        var minY = Math.Min(swY, neY);
        var maxY = Math.Max(swY, neY);

        for (var x = minX; x <= maxX; x++)
            for (var y = minY; y <= maxY; y++)
                yield return (x, y);
    }

    private static Coordinate TileCenter(int x, int y, int zoom)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        return new Coordinate(
            (southWest.Lng + northEast.Lng) / 2,
            (southWest.Lat + northEast.Lat) / 2);
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }
}

/// <summary>Outcome of a bbox-based country re-fetch.</summary>
public record CountryFetchResult(
    int Requested,
    int Resolved,
    int FeaturesUpserted,
    int TilesQueried,
    int TilesFailed,
    int Unmapped);
