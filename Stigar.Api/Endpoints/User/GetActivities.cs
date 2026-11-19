using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Builder;
using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Http;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;

namespace Stigar.Api.Endpoints.User;

public class GetActivities(UserAuthenticationService _userAuthService, IActivityRepository _activitiesRepository)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/activities", (HttpContext context, [FromServices] GetActivities endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        string? sessionId = context.SessionId();
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            return TypedResults.Unauthorized();
        }

        var activities = await _activitiesRepository.GetAllByUser(user.Id);

        var features = activities.Where(a => !string.IsNullOrWhiteSpace(a.SummaryPolyline)).Select(a => a.ToFeature());
        return TypedResults.Ok(new FeatureCollection(features));
    }
}