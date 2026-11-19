using Microsoft.AspNetCore.Mvc;
using API.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using Shared.Services;
using Shared.Services.StravaClient;

namespace API.Endpoints.StravaRoutes;

public class GetStravaRouteGpx(
    UserAuthenticationService _userAuthService,
    StravaTokenService _stravaTokenService,
    RoutesApi _routesApi)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/strava/routes/{routeId}/gpx", (HttpContext context, [FromServices] GetStravaRouteGpx endpoint, string routeId) => endpoint.Handle(context, routeId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string routeId, CancellationToken cancellationToken)
    {
        string? sessionId = context.SessionId();
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return TypedResults.Unauthorized();

        var token = await _stravaTokenService.GetValidAccessToken(user);
        if (token == null)
            return TypedResults.Unauthorized();

        var gpxStream = await _routesApi.GetRouteGpx(token, routeId);
        if (gpxStream == null)
            return TypedResults.NotFound();

        await using (gpxStream)
        {
            context.Response.ContentType = "application/gpx+xml";
            context.Response.Headers["Content-Disposition"] = $"attachment; filename=\"route-{routeId}.gpx\"";
            await gpxStream.CopyToAsync(context.Response.Body, cancellationToken);
            return TypedResults.Empty;
        }
    }
}
