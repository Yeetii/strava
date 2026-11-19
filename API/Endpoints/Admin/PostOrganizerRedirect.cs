using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using API.Http;
using Shared.Services;

namespace API.Endpoints.Admin;

public class PostOrganizerRedirect(
    IOrganizerStore organizerStore,
    IConfiguration configuration,
    ILogger<PostOrganizerRedirect> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/organizers/redirects",
            (HttpContext context, [FromServices] PostOrganizerRedirect endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        PostOrganizerRedirectRequest? body;
        try
        {
            body = await context.Request.ReadFromJsonAsync<PostOrganizerRedirectRequest>(cancellationToken);
        }
        catch
        {
            return BadRequest("Invalid JSON body");
        }

        if (body is null || string.IsNullOrWhiteSpace(body.FromUrl) || string.IsNullOrWhiteSpace(body.ToUrl))
            return BadRequest("Missing required fields: fromUrl and toUrl");

        if (!TryParseHttpUrl(body.FromUrl, out var fromUrl))
            return BadRequest("Invalid fromUrl — must be an absolute http(s) URL");

        if (!TryParseHttpUrl(body.ToUrl, out var toUrl))
            return BadRequest("Invalid toUrl — must be an absolute http(s) URL");

        var sourceOrganizerKey = OrganizerUrlRules.DeriveOrganizerKey(fromUrl);
        var targetOrganizerKey = OrganizerUrlRules.DeriveOrganizerKey(toUrl);

        try
        {
            await organizerStore.SetRedirectAsync(sourceOrganizerKey, targetOrganizerKey, cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(ex.Message);
        }

        var resolvedTargetOrganizerKey = await organizerStore.ResolveOrganizerKeyAsync(targetOrganizerKey, cancellationToken);
        logger.LogInformation(
            "Configured organizer redirect {SourceOrganizerKey} -> {TargetOrganizerKey}",
            sourceOrganizerKey,
            resolvedTargetOrganizerKey);

        return TypedResults.Ok(
            new PostOrganizerRedirectResponse(
                sourceOrganizerKey,
                resolvedTargetOrganizerKey,
                fromUrl.AbsoluteUri,
                toUrl.AbsoluteUri));
    }

    private static bool TryParseHttpUrl(string rawUrl, out Uri parsedUrl)
    {
        if (Uri.TryCreate(rawUrl.Trim(), UriKind.Absolute, out var candidate)
            && candidate.Scheme is "http" or "https")
        {
            parsedUrl = candidate;
            return true;
        }

        parsedUrl = null!;
        return false;
    }

    private static IResult BadRequest(string message) => TypedResults.BadRequest(message);
}

public record PostOrganizerRedirectRequest(string FromUrl, string ToUrl);

public record PostOrganizerRedirectResponse(
    string SourceOrganizerKey,
    string TargetOrganizerKey,
    string FromUrl,
    string ToUrl);
