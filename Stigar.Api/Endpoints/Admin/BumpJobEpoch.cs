using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Api.Http;
using Stigar.Core.Jobs;

namespace Stigar.Api.Endpoints.Admin;

/// <summary>
/// Bumps a job type's epoch so the polling jobs rediscover every row whose stored stage epoch is
/// behind. Returns the new epoch; nothing is rewritten in bulk.
/// </summary>
public class BumpJobEpoch(
    IJobEpochService epochs,
    IConfiguration configuration,
    ILogger<BumpJobEpoch> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/jobs/{jobType}/rediscover",
            (HttpContext context, [FromServices] BumpJobEpoch endpoint, string jobType) =>
                endpoint.Handle(context, jobType));

    public async Task<IResult> Handle(HttpContext context, string jobType)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        if (!JobEpochKeys.TryParse(jobType, out var type))
            return TypedResults.BadRequest(
                $"Unknown job type '{jobType}'. Valid values: {string.Join(", ", JobEpochKeys.All)}");

        var epoch = await epochs.BumpAsync(type, context.RequestAborted);
        logger.LogInformation("Bumped epoch for {JobType} to {Epoch}", jobType, epoch);
        return TypedResults.Ok(new { jobType, epoch });
    }
}
