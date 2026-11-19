using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Shared.Services.Postgres.Admin;
using API.Http;

namespace API.Endpoints.Admin;

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
