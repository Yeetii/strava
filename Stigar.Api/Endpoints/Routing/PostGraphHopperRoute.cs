using System.Text;
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

        // Only forward well-formed JSON, and never echo the raw request body into
        // a response. Both ends are parsed so caller-controlled bytes cannot be
        // rendered as HTML/JavaScript by a downstream client.
        try
        {
            using var requestJson = JsonDocument.Parse(body);
        }
        catch (JsonException)
        {
            return TypedResults.BadRequest("Request body must be valid JSON.");
        }

        using var upstreamRequest = new HttpRequestMessage(HttpMethod.Post, _graphHopperRouteUrl)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        using var upstreamResponse = await httpClient.SendAsync(upstreamRequest, cancellationToken);
        var responseBody = await upstreamResponse.Content.ReadAsStringAsync(cancellationToken);
        var statusCode = (int)upstreamResponse.StatusCode;

        context.Response.Headers["X-Content-Type-Options"] = "nosniff";

        try
        {
            using var responseJson = JsonDocument.Parse(responseBody);
            return TypedResults.Json(responseJson.RootElement.Clone(), statusCode: statusCode);
        }
        catch (JsonException)
        {
            // Upstream returned a non-JSON error page: do not relay it verbatim.
            return TypedResults.StatusCode(statusCode >= 400 ? statusCode : StatusCodes.Status502BadGateway);
        }
    }
}
