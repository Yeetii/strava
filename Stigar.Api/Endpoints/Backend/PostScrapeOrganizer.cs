using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Stigar.Api.Http;
using Stigar.Core.Jobs;
using Stigar.Core.Models;
using Stigar.Core.Services;

namespace Stigar.Api.Endpoints.Backend;

/// <summary>Manually runs the race scrape pipeline for one organizer.</summary>
public class PostScrapeOrganizer(
    IOrganizerStore organizerClient,
    RaceScrapePipeline pipeline,
    IConfiguration configuration)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/scrape/organizer",
            (HttpContext context, [FromServices] PostScrapeOrganizer endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        ScrapeRaceMessage? parsed;
        try
        {
            parsed = await context.Request.ReadFromJsonAsync<ScrapeRaceMessage>(cancellationToken);
        }
        catch
        {
            return TypedResults.BadRequest("Invalid JSON body");
        }

        var organizerKey = parsed?.OrganizerKey?.Trim();
        if (string.IsNullOrWhiteSpace(organizerKey))
            return TypedResults.BadRequest("Missing required field: organizerKey");

        var doc = await organizerClient.GetByIdAsync(organizerKey, cancellationToken);
        if (doc is null)
            return TypedResults.NotFound($"No organizer document for key '{Stigar.Core.Logging.LogInput.Sanitize(organizerKey)}'");

        await pipeline.RunAsync(organizerKey, doc, cancellationToken);

        var updated = await organizerClient.GetByIdAsync(organizerKey, cancellationToken);
        return TypedResults.Ok(updated);
    }
}
