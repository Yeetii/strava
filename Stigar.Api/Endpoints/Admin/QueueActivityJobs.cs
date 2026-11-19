using Microsoft.AspNetCore.Mvc;
using Stigar.Core.Jobs;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Stigar.Api.Http;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;

namespace Stigar.Api.Endpoints.Admin;

public class QueueActivityJobs(
    IActivityRepository activityRepository,
    IBackgroundJobClient jobs,
    UserAuthenticationService userAuthService)
{
    private static readonly Dictionary<string, string> JobTypeToQueue = new(StringComparer.OrdinalIgnoreCase)
    {
        ["summits"] = Stigar.Core.Constants.QueueConfig.CalculateSummitsJobs,
        ["visitedPaths"] = Stigar.Core.Constants.QueueConfig.CalculateVisitedPathsJobs,
        ["visitedAreas"] = Stigar.Core.Constants.QueueConfig.CalculateVisitedAreasJobs,
    };

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/queue/{jobType}",
            (HttpContext context, [FromServices] QueueActivityJobs endpoint, string jobType) => endpoint.Handle(context, jobType));

    public async Task<IResult> Handle(HttpContext context, string jobType)
    {
        string? sessionId = context.SessionId();
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            return TypedResults.Unauthorized();
        }

        if (!JobTypeToQueue.TryGetValue(jobType, out var queueName))
        {
            return TypedResults.BadRequest($"Unknown job type '{jobType}'. Valid values: {string.Join(", ", JobTypeToQueue.Keys)}");
        }

        var queryString = context.Request.Query;
        var targetUserId = queryString["userId"].ToString();
        var effectiveUserId = string.IsNullOrWhiteSpace(targetUserId)
            ? user.Id
            : targetUserId;
        DateTime? startDate = DateTime.TryParse(queryString["startDate"].ToString(), out var sd) ? sd.ToUniversalTime() : null;
        DateTime? endDate = DateTime.TryParse(queryString["endDate"].ToString(), out var ed) ? ed.ToUniversalTime() : null;

        var idList = (await activityRepository.GetAllByUser(effectiveUserId))
            .Where(activity => !startDate.HasValue || activity.StartDate >= startDate.Value)
            .Where(activity => !endDate.HasValue || activity.StartDate <= endDate.Value)
            .Select(activity => activity.Id)
            .ToList();

        await QueueActivityCollectionJobs.QueueActivityIdsAsync(idList, jobs, queueName, context.RequestAborted);

        return TypedResults.Ok(idList.Count);
    }
}
