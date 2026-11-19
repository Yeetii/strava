using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Configuration;
using Stigar.Api.Utils;
using Stigar.Core.Geo;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;

namespace Stigar.Api.Endpoints.AdminBoundaries;

public class GetAdminBoundariesByGrid(
    IAdminBoundaryRepository adminBoundaries,
    IOsmFeatureIngestionService ingestionService,
    IOsmTileQueryRepository tileQueries,
    IConfiguration configuration)
{
    private const int DefaultZoom = 6;
    private const int DefaultAdminLevel = 2;
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/boundaries/{x}/{y}", (HttpContext context, [FromServices] GetAdminBoundariesByGrid endpoint, int x, int y) => endpoint.Handle(context, x, y, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, int x, int y, CancellationToken cancellationToken)
    {
        try
        {
            var zoom = ParseIntQuery(context, "zoom", DefaultZoom);
            var adminLevel = ParseIntQuery(context, "adminLevel", DefaultAdminLevel);
            var forceRefresh = string.Equals(context.Request.Query["forceRefresh"], "true", StringComparison.OrdinalIgnoreCase);

            if (forceRefresh && !context.IsAdminAuthorized(configuration))
                return TypedResults.Unauthorized();

            if (forceRefresh)
                await RefreshTileAsync(x, y, zoom, adminLevel, cancellationToken);

            var boundaries = await adminBoundaries.GetByTile(x, y, adminLevel, zoom, cancellationToken);

            var featureCollection = new FeatureCollection(boundaries
                .Select(b => b.ToFeature()).ToList());

            return TypedResults.Ok(featureCollection);
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return TypedResults.NoContent();
        }
    }

    private async Task RefreshTileAsync(int x, int y, int zoom, int adminLevel, CancellationToken cancellationToken)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var storageZoom = AdminOsmConstants.AdminBoundaryStorageZoom;

        var (swX, swY) = SlippyTileCalculator.WGS84ToTileIndex(southWest, storageZoom);
        var (neX, neY) = SlippyTileCalculator.WGS84ToTileIndex(northEast, storageZoom);

        for (var tileX = Math.Min(swX, neX); tileX <= Math.Max(swX, neX); tileX++)
        {
            for (var tileY = Math.Min(swY, neY); tileY <= Math.Max(swY, neY); tileY++)
            {
                await tileQueries.Delete(
                    FeatureKinds.AdminBoundary,
                    storageZoom,
                    tileX,
                    tileY,
                    cancellationToken);
            }
        }

        var center = new Coordinate(
            (southWest.Lng + northEast.Lng) / 2,
            (southWest.Lat + northEast.Lat) / 2);
        await ingestionService.EnsureAdminBoundariesAsync(
            [southWest, northEast, center],
            adminLevel,
            cancellationToken);
    }


    private static int ParseIntQuery(HttpContext req, string name, int defaultValue)
    {
        return int.TryParse(req.Request.Query[name], out var value) ? value : defaultValue;
    }
}
