using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Api.Http;
using Stigar.Core.Jobs;

namespace Stigar.Api.Endpoints.Admin;

public record JobEpochStatus(string JobType, long Epoch);

/// <summary>Exposes the current epoch of every rediscoverable job type to the admin UI.</summary>
public class GetJobEpochs(IJobEpochService epochs, IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/manage/jobs/epochs",
            (HttpContext context, [FromServices] GetJobEpochs endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var values = await epochs.GetAllAsync(context.RequestAborted);
        return TypedResults.Ok(values
            .Select(value => new JobEpochStatus(value.JobType, value.Epoch))
            .ToArray());
    }
}
