using Microsoft.AspNetCore.Mvc;
using API.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace API.Endpoints.Routing;

public class PostGraphHopperRoute(HttpClient httpClient, IConfiguration configuration)
{
    private readonly string _graphHopperRouteUrl =
        configuration.GetValue<string>("GraphHopperRouteUrl")
        ?? "https://graphhopper.gpx.studio/route";
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/route", ["POST", "OPTIONS"], (HttpContext context, [FromServices] PostGraphHopperRoute endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (HttpMethods.IsOptions(context.Request.Method))
            return TypedResults.NoContent();

        var body = await new StreamReader(context.Request.Body).ReadToEndAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(body))
            return TypedResults.BadRequest("Request body is required.");

        using var upstreamRequest = new HttpRequestMessage(HttpMethod.Post, _graphHopperRouteUrl)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        using var upstreamResponse = await httpClient.SendAsync(upstreamRequest, cancellationToken);
        var responseBody = await upstreamResponse.Content.ReadAsStringAsync(cancellationToken);
        if (upstreamResponse.Content.Headers.ContentType is { } contentType)
            context.Response.ContentType = contentType.ToString();
        context.Response.StatusCode = (int)upstreamResponse.StatusCode;
        return TypedResults.Content(responseBody, context.Response.ContentType);
    }


}