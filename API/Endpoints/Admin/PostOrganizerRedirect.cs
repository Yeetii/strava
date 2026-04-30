using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.Admin;

public class PostOrganizerRedirect(
    BlobOrganizerStore organizerStore,
    IConfiguration configuration,
    ILogger<PostOrganizerRedirect> logger)
{
    [OpenApiOperation(tags: ["Admin"], Summary = "Redirect one organizer site to another organizer id.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(PostOrganizerRedirectRequest))]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(PostOrganizerRedirectResponse))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [Function(nameof(PostOrganizerRedirect))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/organizers/redirects")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        PostOrganizerRedirectRequest? body;
        try
        {
            body = await req.ReadFromJsonAsync<PostOrganizerRedirectRequest>(cancellationToken);
        }
        catch
        {
            return await BadRequest(req, "Invalid JSON body", cancellationToken);
        }

        if (body is null || string.IsNullOrWhiteSpace(body.FromUrl) || string.IsNullOrWhiteSpace(body.ToUrl))
            return await BadRequest(req, "Missing required fields: fromUrl and toUrl", cancellationToken);

        if (!TryParseHttpUrl(body.FromUrl, out var fromUrl))
            return await BadRequest(req, "Invalid fromUrl — must be an absolute http(s) URL", cancellationToken);

        if (!TryParseHttpUrl(body.ToUrl, out var toUrl))
            return await BadRequest(req, "Invalid toUrl — must be an absolute http(s) URL", cancellationToken);

        var sourceOrganizerKey = BlobOrganizerStore.DeriveOrganizerKey(fromUrl);
        var targetOrganizerKey = BlobOrganizerStore.DeriveOrganizerKey(toUrl);

        try
        {
            await organizerStore.SetRedirectAsync(sourceOrganizerKey, targetOrganizerKey, cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            return await BadRequest(req, ex.Message, cancellationToken);
        }

        var resolvedTargetOrganizerKey = await organizerStore.ResolveOrganizerKeyAsync(targetOrganizerKey, cancellationToken);
        logger.LogInformation(
            "Configured organizer redirect {SourceOrganizerKey} -> {TargetOrganizerKey}",
            sourceOrganizerKey,
            resolvedTargetOrganizerKey);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(
            new PostOrganizerRedirectResponse(
                sourceOrganizerKey,
                resolvedTargetOrganizerKey,
                fromUrl.AbsoluteUri,
                toUrl.AbsoluteUri),
            cancellationToken);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
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

    private static async Task<HttpResponseData> BadRequest(HttpRequestData req, string message, CancellationToken cancellationToken)
    {
        var response = req.CreateResponse(HttpStatusCode.BadRequest);
        await response.WriteStringAsync(message, cancellationToken);
        return response;
    }
}

public record PostOrganizerRedirectRequest(string FromUrl, string ToUrl);

public record PostOrganizerRedirectResponse(
    string SourceOrganizerKey,
    string TargetOrganizerKey,
    string FromUrl,
    string ToUrl);