using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using Shared.Services.StravaClient;

namespace API.Endpoints.StravaRoutes;

public class GetStravaRouteGpx(
    UserAuthenticationService _userAuthService,
    StravaTokenService _stravaTokenService,
    RoutesApi _routesApi)
{
    [OpenApiOperation(tags: ["Strava Routes"], Summary = "Export a Strava route as a GPX file.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "routeId", In = ParameterLocation.Path, Type = typeof(long), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/gpx+xml", bodyType: typeof(string),
        Description = "The route as a GPX file.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NotFound, Description = "Route not found")]
    [Function(nameof(GetStravaRouteGpx))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "strava/routes/{routeId}/gpx")] HttpRequestData req,
        long routeId)
    {
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var token = await _stravaTokenService.GetValidAccessToken(user);
        if (token == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var gpxStream = await _routesApi.GetRouteGpx(token, routeId);
        if (gpxStream == null)
            return req.CreateResponse(HttpStatusCode.NotFound);

        await using (gpxStream)
        {
            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "application/gpx+xml");
            response.Headers.Add("Content-Disposition", $"attachment; filename=\"route-{routeId}.gpx\"");
            await gpxStream.CopyToAsync(response.Body);
            return response;
        }
    }
}
