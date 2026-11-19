using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;

namespace Stigar.Api.Endpoints.Routing;

public class PostGraphHopperRoute(HttpClient httpClient, IConfiguration configuration)
{
    private readonly string _graphHopperRouteUrl =
        configuration.GetValue<string>("GraphHopperRouteUrl")
        ?? "https://graphhopper.gpx.studio/route";

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/route", ["POST", "OPTIONS"],
            (HttpContext context, [FromServices] PostGraphHopperRoute endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (HttpMethods.IsOptions(context.Request.Method))
            return TypedResults.NoContent();

        var body = await new StreamReader(context.Request.Body).ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(body))
            return TypedResults.BadRequest("Request body is required.");

        // Parse the caller's bytes into structured JSON before forwarding them.
        // No raw request string ever reaches a response or an HTML/JS sink.
        JsonElement requestJson;
        try
        {
            using var document = JsonDocument.Parse(body);
            requestJson = document.RootElement.Clone();
        }
        catch (JsonException)
        {
            return TypedResults.BadRequest("Request body must be valid JSON.");
        }

        using var upstreamRequest = new HttpRequestMessage(HttpMethod.Post, _graphHopperRouteUrl)
        {
            Content = JsonContent.Create(requestJson),
        };

        using var upstreamResponse = await httpClient.SendAsync(upstreamRequest, cancellationToken);
        var statusCode = (int)upstreamResponse.StatusCode;
        context.Response.Headers["X-Content-Type-Options"] = "nosniff";

        JsonElement responseJson;
        try
        {
            responseJson = await upstreamResponse.Content.ReadFromJsonAsync<JsonElement>(cancellationToken);
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            // Upstream returned a non-JSON error page: do not relay it verbatim.
            return TypedResults.StatusCode(statusCode >= 400 ? statusCode : StatusCodes.Status502BadGateway);
        }

        return TypedResults.Json(responseJson, statusCode: statusCode);
    }
}
