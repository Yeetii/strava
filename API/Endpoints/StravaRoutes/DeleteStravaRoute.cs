using System.Net;
using API.Utils;
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
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "Route deleted successfully or CORS preflight")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound, Description = "Route not found")]
    [Function(nameof(DeleteStravaRoute))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", "options", Route = "strava/routes/{routeId}")] HttpRequestData req,
        string routeId)
    {
        if (CorsHeaders.IsOptions(req))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            CorsHeaders.Add(req, optionsResponse, "DELETE, OPTIONS");
            return optionsResponse;
        }

        var response = req.CreateResponse();
        CorsHeaders.Add(req, response, "DELETE, OPTIONS");

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

        var deleted = await _routesApi.DeleteRoute(token, routeId);
        if (!deleted)
        {
            response.StatusCode = HttpStatusCode.NotFound;
            return response;
        }

        response.StatusCode = HttpStatusCode.NoContent;
        return response;
    }
}
