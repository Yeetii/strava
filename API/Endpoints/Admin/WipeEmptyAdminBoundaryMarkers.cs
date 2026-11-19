using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using API.Http;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Admin;

public class WipeEmptyAdminBoundaryMarkers(
    IOsmFeatureAdminQueries adminQueries,
    IConfiguration configuration,
    ILogger<WipeEmptyAdminBoundaryMarkers> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapDelete("/api/manage/adminBoundaries/emptyMarkers",
            (HttpContext context, [FromServices] WipeEmptyAdminBoundaryMarkers endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var adminLevelParam = context.Request.Query["adminLevel"].ToString();
        if (!string.IsNullOrWhiteSpace(adminLevelParam) && int.TryParse(adminLevelParam, out var adminLevel))
        {
            logger.LogWarning(
                "adminLevel={AdminLevel} cannot narrow the wipe: osm_tile_queries has no admin_level column; all adminBoundary tile-query markers will be deleted",
                adminLevel);
        }

        var deleted = await adminQueries.DeleteOsmTileQueriesAsync();

        return TypedResults.Ok(deleted);
    }
}
