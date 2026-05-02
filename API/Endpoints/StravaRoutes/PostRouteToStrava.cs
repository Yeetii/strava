using System.Net;
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
    [OpenApiOperation(tags: ["Strava Routes"], Summary = "Upload a GPX route to Strava as an activity.",
        Description = "Send a GPX file in the request body. Provide 'name' (required) and optionally 'description' as query parameters.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "name", In = ParameterLocation.Query, Type = typeof(string), Required = true,
        Description = "Name for the activity on Strava.")]
    [OpenApiParameter(name: "description", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional description for the activity.")]
    [OpenApiParameter(name: "filename", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Filename hint including extension (e.g. 'my-route.gpx'). Determines the data_type sent to Strava. Defaults to 'route.gpx'.")]
    [OpenApiRequestBody(contentType: "application/octet-stream", bodyType: typeof(string),
        Description = "Raw GPX (or TCX/FIT) file content.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(UploadStatus),
        Description = "The Strava upload status object.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.BadRequest, contentType: "text/plain", bodyType: typeof(string),
        Description = "Missing required query parameters.")]
    [Function(nameof(PostRouteToStrava))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "strava/routes")] HttpRequestData req)
    {
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var token = await _stravaTokenService.GetValidAccessToken(user);
        if (token == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var query = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var name = query["name"];
        if (string.IsNullOrWhiteSpace(name))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            await badRequest.WriteStringAsync("Query parameter 'name' is required");
            return badRequest;
        }

        var description = query["description"];
        var filename = query["filename"] ?? "route.gpx";
        var extension = Path.GetExtension(filename).TrimStart('.').ToLowerInvariant();
        var dataType = extension is "gpx" or "gpx.gz" or "tcx" or "tcx.gz" or "fit" or "fit.gz" ? extension : "gpx";

        var uploadStatus = await _routesApi.UploadActivity(token, req.Body, filename, name, dataType, description);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(uploadStatus);
        return response;
    }
}
