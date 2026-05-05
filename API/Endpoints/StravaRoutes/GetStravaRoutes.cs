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

public class GetStravaRoutes(
    UserAuthenticationService _userAuthService,
    StravaTokenService _stravaTokenService,
    RoutesApi _routesApi)
{
    private sealed record RouteSummaryDto(
        long Id,
        string IdStr,
        string Name,
        string? Description,
        float? DistanceMeters,
        float? ElevationGain,
        string ActivityType,
        DateTime? CreatedAt,
        DateTime? UpdatedAt,
        bool? Private,
        bool? Starred,
        string? SummaryPolyline);

    [OpenApiOperation(tags: ["Strava Routes"], Summary = "List the authenticated user's Strava routes.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<RouteSummaryDto>),
        Description = "A list of the user's Strava routes.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.NoContent, Description = "CORS preflight response")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized, Description = "Session is missing or invalid")]
    [Function(nameof(GetStravaRoutes))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", "options", Route = "strava/routes")] HttpRequestData req)
    {
        if (CorsHeaders.IsOptions(req))
        {
            var optionsResponse = req.CreateResponse(HttpStatusCode.NoContent);
            CorsHeaders.Add(req, optionsResponse, "GET, OPTIONS");
            return optionsResponse;
        }

        var response = req.CreateResponse();
        CorsHeaders.Add(req, response, "GET, OPTIONS");

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

        var routes = await _routesApi.GetAthleteRoutes(token, user.Id);
        if (routes == null)
        {
            response.StatusCode = HttpStatusCode.NotFound;
            return response;
        }

        var dtos = routes.Select(r => new RouteSummaryDto(
            r.Id,
            r.IdStr,
            r.Name,
            r.Description,
            r.Distance,
            r.ElevationGain,
            MapActivityType(r),
            r.CreatedAt,
            r.UpdatedAt,
            r.Private,
            r.Starred,
            r.Map?.SummaryPolyline));

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(dtos);
        return response;
    }

    private static string MapActivityType(StravaRoute route)
    {
        if (route.Segments?.Any(segment => string.Equals(segment.ActivityType, "Run", StringComparison.OrdinalIgnoreCase)) == true)
            return "Run/hike";

        return (route.Type, route.SubType) switch
        {
            (1, 1) => "Road bike",
            (1, 2) => "Mountain bike",
            (1, 3) => "Gravel bike",
            (1, 4) => "Mountain bike",
            (1, 5) => "Bike",
            (2, _) => "Run/hike",
            (5, _) => "Run/hike",
            (6, _) => "Gravel bike",
            (1, _) => "Bike",
            _ => "Run/hike"
        };
    }
}
