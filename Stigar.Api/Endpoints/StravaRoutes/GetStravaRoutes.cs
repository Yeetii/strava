using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using Stigar.Api.Utils;
using Stigar.Core.Services;
using Stigar.Core.Services.StravaClient;
using Stigar.Core.Services.StravaClient.Model;

namespace Stigar.Api.Endpoints.StravaRoutes;

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
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods("/api/strava/routes", ["GET", "OPTIONS"], (HttpContext context, [FromServices] GetStravaRoutes endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (context.IsOptionsRequest())
            return TypedResults.NoContent();

        string? sessionId = context.SessionId();
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return TypedResults.Unauthorized();

        var token = await _stravaTokenService.GetValidAccessToken(user);
        if (token == null)
            return TypedResults.Unauthorized();

        var routes = await _routesApi.GetAthleteRoutes(token, user.Id);
        if (routes == null)
            return TypedResults.NotFound();

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
        return TypedResults.Ok(dtos);
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
