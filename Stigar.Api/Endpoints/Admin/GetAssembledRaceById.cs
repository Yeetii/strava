using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

public class GetAssembledRaceById(
    IOrganizerStore organizerStore,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/races/assembled/{featureId}",
            (HttpContext context, [FromServices] GetAssembledRaceById endpoint, string featureId) => endpoint.Handle(context, featureId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string featureId, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var organizerKey = featureId.Contains('-', StringComparison.Ordinal)
            ? featureId[..featureId.LastIndexOf('-')]
            : featureId;
        var feature = await organizerStore.GetAssembledRaceAsync(organizerKey, featureId, cancellationToken);
        if (feature is null)
            return TypedResults.NotFound();

        return TypedResults.Ok(feature);
    }
}
