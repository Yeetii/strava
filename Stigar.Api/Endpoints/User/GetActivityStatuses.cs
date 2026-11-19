using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;

namespace Stigar.Api.Endpoints.User;

public sealed class ActivityStatusProjection
{
    public string Id { get; set; } = string.Empty;
    public string? Name { get; set; }
    public DateTime StartDateLocal { get; set; }
    public string? SummaryPolyline { get; set; }
    public ActivityProcessingStatus? ProcessingStatus { get; set; }
}

public class GetActivityStatuses
{
    private const int DefaultLimit = 10;
    private const int MaxLimit = 100;

    private readonly UserAuthenticationService _userAuthService;
    private readonly IActivityRepository _activitiesRepository;

    public GetActivityStatuses(UserAuthenticationService userAuthService, IActivityRepository activitiesRepository)
    {
        _userAuthService = userAuthService;
        _activitiesRepository = activitiesRepository;
    }

    private sealed record ActivityStatusDto(
        string ActivityId,
        string ActivityName,
        DateTime StartDateLocal,
        string? SummaryPolyline,
        ActivityProcessingStatus ProcessingStatus);
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/activityStatuses", (HttpContext context, [FromServices] GetActivityStatuses endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        var sessionId = context.SessionId();
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return TypedResults.Unauthorized();

        var limit = ParseLimit(context.Request.Query["limit"]);
        var projected = (await _activitiesRepository.GetAllByUser(user.Id))
            .OrderByDescending(activity => activity.StartDateLocal)
            .Take(limit)
            .Select(activity => new ActivityStatusProjection
            {
                Id = activity.Id,
                Name = activity.Name,
                StartDateLocal = activity.StartDateLocal,
                SummaryPolyline = activity.SummaryPolyline,
                ProcessingStatus = activity.ProcessingStatus,
            });

        var statuses = projected
            .Select(activity => new ActivityStatusDto(
                ActivityId: activity.Id,
                ActivityName: activity.Name ?? string.Empty,
                StartDateLocal: activity.StartDateLocal,
                SummaryPolyline: activity.SummaryPolyline,
                ProcessingStatus: activity.ProcessingStatus ?? new ActivityProcessingStatus()))
            .ToArray();

        return TypedResults.Ok(statuses);
    }

    public static int ParseLimit(string? rawLimit)
    {
        if (!int.TryParse(rawLimit, out var requestedLimit))
            return DefaultLimit;

        if (requestedLimit < 1)
            return 1;

        return requestedLimit > MaxLimit ? MaxLimit : requestedLimit;
    }
}
