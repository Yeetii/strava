using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Core.Geo;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

public class FetchCountries(
    IOsmFeatureIngestionService ingestionService,
    IOsmFeatureAdminQueries adminQueries,
    IOsmTileQueryRepository tileQueries,
    IConfiguration configuration,
    ILogger<FetchCountries> logger)
{
    private const int CountryAdminLevel = 2;

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/countries/fetch",
            (HttpContext context, [FromServices] FetchCountries endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var osmIdsParam = context.Request.Query["osmIds"].ToString();
        var codesParam = context.Request.Query["countryCodes"].ToString();

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

        return TypedResults.Ok(result);
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
}

/// <summary>Outcome of a bbox-based country re-fetch.</summary>
public record CountryFetchResult(
    int Requested,
    int Resolved,
    int FeaturesUpserted,
    int TilesQueried,
    int TilesFailed,
    int Unmapped);
