using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

public class GetCorruptedAdminBoundaries(
    IOsmFeatureAdminQueries adminQueries,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/adminBoundaries/corrupted",
            (HttpContext context, [FromServices] GetCorruptedAdminBoundaries endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var ids = await adminQueries.GetCorruptedAdminBoundaryIdsAsync();

        return TypedResults.Ok(ids);
    }
}
