using Microsoft.AspNetCore.Mvc;
using Stigar.Core.Jobs;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Api.Http;
using Stigar.Core.Constants;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Api.Endpoints.Admin;

public class ReEnrichAdminBoundaries(
    IOsmFeatureAdminQueries adminQueries,
    IBackgroundJobClient jobs,
    IConfiguration configuration,
    ILogger<ReEnrichAdminBoundaries> logger)
{

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/adminBoundaries/reEnrich",
            (HttpContext context, [FromServices] ReEnrichAdminBoundaries endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var ids = await adminQueries.GetAllAdminBoundaryIdsAsync();

        logger.LogInformation("Queuing {Count} admin boundary enrichment jobs", ids.Count);

        foreach (var id in ids)
            jobs.EnqueueAdminBoundaryEnrichment(AdminBoundaryIdJob.From(id));

        return TypedResults.Ok(ids.Count);
    }
}
