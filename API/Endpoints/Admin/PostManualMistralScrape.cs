using Microsoft.AspNetCore.Mvc;
using Hangfire;
using Shared.Jobs;
using System.Linq;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using API.Http;

namespace API.Endpoints.Admin;

public class PostManualMistralScrape(
    IBackgroundJobClient jobs,
    IConfiguration configuration,
    ILogger<PostManualMistralScrape> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/races/scrape/mistral",
            (HttpContext context, [FromServices] PostManualMistralScrape endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        PostManualMistralScrapeRequest? body;
        try
        {
            body = await context.Request.ReadFromJsonAsync<PostManualMistralScrapeRequest>(cancellationToken);
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

        jobs.EnqueueManualMistralScrape(new ManualMistralScrapeJob(parsedUrl.AbsoluteUri, body.Name));
        logger.LogInformation("Enqueued manual mistral scrape job for {OrganizerKey}", organizerKey);

        return TypedResults.Ok(new PostManualMistralScrapeResponse(organizerKey, parsedUrl.AbsoluteUri));
    }
}

public record PostManualMistralScrapeRequest(string Url, string? Name = null);
public record PostManualMistralScrapeResponse(string EventKey, string CanonicalUrl);
