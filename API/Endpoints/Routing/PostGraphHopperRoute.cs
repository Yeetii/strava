using System.Net;
using System.Text;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;

namespace API.Endpoints.Routing;

public class PostGraphHopperRoute(HttpClient httpClient, IConfiguration configuration)
{
    private readonly string _graphHopperRouteUrl =
        configuration.GetValue<string>("GraphHopperRouteUrl")
        ?? "https://graphhopper.gpx.studio/route";

    [OpenApiOperation(tags: ["Routing"])]
    [OpenApiRequestBody(contentType: "application/json", bodyType: typeof(object), Required = true)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(object), Description = "GraphHopper route response")]
    [Function(nameof(PostGraphHopperRoute))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", "options", Route = "route")] HttpRequestData req)
    {
        if (HttpMethods.IsOptions(req.Method))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            AddCorsHeaders(req, optionsResponse);
            return optionsResponse;
        }

        var body = await new StreamReader(req.Body).ReadToEndAsync();
        if (string.IsNullOrWhiteSpace(body))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            AddCorsHeaders(req, badRequest);
            await badRequest.WriteStringAsync("Request body is required.");
            return badRequest;
        }

        using var upstreamRequest = new HttpRequestMessage(HttpMethod.Post, _graphHopperRouteUrl)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };

        using var upstreamResponse = await httpClient.SendAsync(upstreamRequest);
        var response = req.CreateResponse(upstreamResponse.StatusCode);
        AddCorsHeaders(req, response);

        if (upstreamResponse.Content.Headers.ContentType is { } contentType)
        {
            response.Headers.Add("Content-Type", contentType.ToString());
        }

        var responseBody = await upstreamResponse.Content.ReadAsStringAsync();
        await response.WriteStringAsync(responseBody);
        return response;
    }

    private static void AddCorsHeaders(HttpRequestData req, HttpResponseData response)
    {
        response.Headers.Add("Access-Control-Allow-Credentials", "true");
        response.Headers.Add("Access-Control-Allow-Headers", "Content-Type");
        response.Headers.Add("Access-Control-Allow-Methods", "POST, OPTIONS");

        var origin = req.Headers.TryGetValues("Origin", out var origins)
            ? origins.FirstOrDefault()
            : null;

        if (!string.IsNullOrWhiteSpace(origin))
        {
            response.Headers.Add("Access-Control-Allow-Origin", origin);
            response.Headers.Add("Vary", "Origin");
        }
    }

    private static class HttpMethods
    {
        public static bool IsOptions(string method) =>
            string.Equals(method, "OPTIONS", StringComparison.OrdinalIgnoreCase);
    }
}