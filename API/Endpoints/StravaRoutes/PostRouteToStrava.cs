using System.Net;
using API.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using Shared.Services.StravaClient.Model;

namespace API.Endpoints.StravaRoutes;

public class PostRouteToStrava(
    UserAuthenticationService _userAuthService,
    StravaTokenService _stravaTokenService,
    RoutesApi _routesApi)
{
    [OpenApiOperation(tags: ["Strava Routes"], Summary = "Upload a GPX file to Strava as an activity.",
        Description = "Strava's public API does not support creating planned routes from GPX. This endpoint uploads the GPX to Strava's uploads API. Provide 'name' (required) and optionally 'description' and 'filename' as query parameters.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "name", In = ParameterLocation.Query, Type = typeof(string), Required = true,
        Description = "Name for the resulting Strava activity.")]
    [OpenApiParameter(name: "description", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional description for the resulting Strava activity.")]
    [OpenApiParameter(name: "filename", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional filename used for the uploaded GPX.")]
    [OpenApiRequestBody(contentType: "application/octet-stream", bodyType: typeof(string),
        Description = "Raw GPX file content.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(StravaUpload),
        Description = "The Strava upload resource.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.BadRequest, contentType: "text/plain", bodyType: typeof(string),
        Description = "Missing required query parameters.")]
    [Function(nameof(PostRouteToStrava))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "strava/routes")] HttpRequestData req)
    {
        var response = req.CreateResponse();
        CorsHeaders.Add(req, response, "POST, OPTIONS");

        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        var token = await _stravaTokenService.GetValidAccessToken(user);
        if (token == null)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var name = query["name"];
        if (string.IsNullOrWhiteSpace(name))
        {
            response.StatusCode = HttpStatusCode.BadRequest;
            await response.WriteStringAsync("Query parameter 'name' is required");
            return response;
        }

        var description = query["description"];
        var filename = query["filename"] ?? "route.gpx";

        var uploadResult = await _routesApi.UploadGpxActivity(token, req.Body, filename, name, description);
        if (uploadResult.Unauthorized)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(uploadResult.Upload);
        return response;
    }
}
