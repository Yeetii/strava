using Microsoft.AspNetCore.Mvc;
using Shared.Jobs;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using API.Http;
using Shared.Constants;
using Shared.Services;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Admin;

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
