using Microsoft.AspNetCore.Mvc;
using Stigar.Core.Jobs;
using Hangfire;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Core.Constants;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

public record PostRaceDiscoveryRequest(string Agent, int? PageStart = null, int? PageEnd = null);
public record PostRaceDiscoveryResponse(string Agent, int PageStart, int PageEnd, int MessagesEnqueued);

public class PostRaceDiscovery(
    IBackgroundJobClient jobs,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/races/discovery",
            (HttpContext context, [FromServices] PostRaceDiscovery endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        PostRaceDiscoveryRequest? body;
        try
        {
            body = await context.Request.ReadFromJsonAsync<PostRaceDiscoveryRequest>(cancellationToken);
        }
        catch
        {
            return TypedResults.BadRequest("Invalid JSON body");
        }

        if (body is null || string.IsNullOrWhiteSpace(body.Agent))
            return TypedResults.BadRequest("Missing required field: agent");

        var agent = body.Agent.Trim().ToLowerInvariant();
        var pageStart = Math.Max(1, body.PageStart ?? 1);
        var pageEnd = Math.Max(pageStart, body.PageEnd ?? pageStart);
        if (pageEnd - pageStart > 49)
            return TypedResults.BadRequest("Refusing to enqueue more than 50 discovery pages in one request");

        for (var page = pageStart; page <= pageEnd; page++)
            jobs.EnqueueRaceDiscovery(new RaceDiscoveryMessage(agent, page));

        return TypedResults.Ok(new PostRaceDiscoveryResponse(agent, pageStart, pageEnd, pageEnd - pageStart + 1));
    }
}
