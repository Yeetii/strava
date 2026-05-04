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
    [OpenApiOperation(tags: ["Strava Routes"], Summary = "Create a Strava route from a GPX file.",
        Description = "Send a GPX file in the request body. Provide 'name' (required), 'type' (1=Ride, 2=Run, default 2), 'subType' (1=Road, 2=MTB, 3=Cross, 4=Trail, 5=Mixed, default 4) and optionally 'description' as query parameters.")]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "name", In = ParameterLocation.Query, Type = typeof(string), Required = true,
        Description = "Name for the route on Strava.")]
    [OpenApiParameter(name: "type", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "Route type: 1 = Ride, 2 = Run. Defaults to 2 (Run).")]
    [OpenApiParameter(name: "subType", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "Route sub-type: 1 = Road, 2 = MTB, 3 = Cross, 4 = Trail, 5 = Mixed. Defaults to 4 (Trail).")]
    [OpenApiParameter(name: "description", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional description for the route.")]
    [OpenApiRequestBody(contentType: "application/octet-stream", bodyType: typeof(string),
        Description = "Raw GPX file content.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(StravaRoute),
        Description = "The created Strava route.")]
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
        var type = int.TryParse(query["type"], out var parsedType) ? parsedType : 2;
        var subType = int.TryParse(query["subType"], out var parsedSubType) ? parsedSubType : 4;

        var filename = query["filename"] ?? "route.gpx";

        var route = await _routesApi.CreateRoute(token, req.Body, filename, name, type, subType, description);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(route);
        return response;
    }
}
