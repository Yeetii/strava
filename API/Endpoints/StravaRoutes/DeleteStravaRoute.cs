using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using Shared.Services.StravaClient;

namespace API.Endpoints.StravaRoutes;

public class DeleteStravaRoute(
    UserAuthenticationService _userAuthService,
    StravaTokenService _stravaTokenService,
    RoutesApi _routesApi)
{
    [OpenApiOperation(tags: ["Strava Routes"], Summary = "Delete a Strava route by ID.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "routeId", In = ParameterLocation.Path, Type = typeof(string), Required = true)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "Route deleted successfully")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound, Description = "Route not found")]
    [Function(nameof(DeleteStravaRoute))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "strava/routes/{routeId}")] HttpRequestData req,
        string routeId)
    {
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var token = await _stravaTokenService.GetValidAccessToken(user);
        if (token == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var deleted = await _routesApi.DeleteRoute(token, routeId);
        if (!deleted)
            return req.CreateResponse(HttpStatusCode.NotFound);

        return req.CreateResponse(HttpStatusCode.NoContent);
    }
}
