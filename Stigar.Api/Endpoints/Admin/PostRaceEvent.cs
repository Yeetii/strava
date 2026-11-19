using Microsoft.AspNetCore.Mvc;
using Stigar.Core.Jobs;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Core.Constants;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Api.Http;

namespace Stigar.Api.Endpoints.Admin;

public class PostRaceEvent(
    IOrganizerStore organizerClient,
    IBackgroundJobClient jobs,
    IConfiguration configuration,
    ILogger<PostRaceEvent> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/races/events",
            (HttpContext context, [FromServices] PostRaceEvent endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        PostRaceEventRequest? body;
        try
        {
            body = await context.Request.ReadFromJsonAsync<PostRaceEventRequest>(cancellationToken);
        }
        catch
        {
            return TypedResults.BadRequest("Invalid JSON body");
        }

        if (body is null || string.IsNullOrWhiteSpace(body.Url))
            return TypedResults.BadRequest("Missing required field: url");

        if (!Uri.TryCreate(body.Url.Trim(), UriKind.Absolute, out var parsedUrl) || parsedUrl.Scheme is not ("http" or "https"))
            return TypedResults.BadRequest("Invalid URL — must be an absolute http(s) URL");

        var organizerKey = OrganizerUrlRules.DeriveOrganizerKey(parsedUrl);
        var discovery = new SourceDiscovery
        {
            DiscoveredAtUtc = DateTime.UtcNow.ToString("o"),
            Name = body.Name,
            SourceUrls = [parsedUrl.AbsoluteUri],
        };

        await organizerClient.WriteDiscoveryAsync(organizerKey, parsedUrl.AbsoluteUri, "manual", [discovery], cancellationToken);
        logger.LogInformation("Manual discovery: wrote organizer {OrganizerKey} from {Url}", Stigar.Core.Logging.LogInput.Sanitize(organizerKey), parsedUrl);

        // Enqueue a scrape message so the scraper picks it up.
        if (body.Scrape != false)
        {
            jobs.EnqueueScrapeRace(new ScrapeRaceMessage(organizerKey));
            logger.LogInformation("Manual discovery: enqueued scrape for {OrganizerKey}", Stigar.Core.Logging.LogInput.Sanitize(organizerKey));
        }

        return TypedResults.Ok(new PostRaceEventResponse(organizerKey, parsedUrl.AbsoluteUri));
    }
}

public record PostRaceEventRequest(string Url, string? Name = null, bool? Scrape = true);
public record PostRaceEventResponse(string EventKey, string CanonicalUrl);
