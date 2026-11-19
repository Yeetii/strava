using Microsoft.AspNetCore.Mvc;
using Shared.Jobs;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using API.Http;
using Shared.Constants;
using Shared.Services;
using Shared.Services.Postgres;

namespace API.Endpoints.Admin;

public class QueueUnprocessedActivityJobs(
    IBackgroundJobClient jobs,
    UserAuthenticationService userAuthService,
    IActivityRepository activityRepository)
{
    private static readonly Dictionary<string, string> JobTypeToQueue = new(StringComparer.OrdinalIgnoreCase)
    {
        ["summits"] = QueueConfig.CalculateSummitsJobs,
        ["visitedPaths"] = QueueConfig.CalculateVisitedPathsJobs,
        ["visitedAreas"] = QueueConfig.CalculateVisitedAreasJobs,
    };

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/queue/unprocessed/{jobType}",
            (HttpContext context, [FromServices] QueueUnprocessedActivityJobs endpoint, string jobType) => endpoint.Handle(context, jobType));

    public async Task<IResult> Handle(HttpContext context, string jobType)
    {
        var sessionId = context.SessionId();
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
        {
            return TypedResults.Unauthorized();
        }

        if (!JobTypeToQueue.TryGetValue(jobType, out var queueName))
        {
            return TypedResults.BadRequest($"Unknown job type '{jobType}'. Valid values: {string.Join(", ", JobTypeToQueue.Keys)}");
        }

        var queryString = context.Request.Query;
        var targetUserId = queryString["userId"].ToString();
        var effectiveUserId = string.IsNullOrWhiteSpace(targetUserId) ? user.Id : targetUserId;
        var startDate = DateTime.TryParse(queryString["startDate"].ToString(), out var sd) ? sd.ToUniversalTime() : (DateTime?)null;
        var endDate = DateTime.TryParse(queryString["endDate"].ToString(), out var ed) ? ed.ToUniversalTime() : (DateTime?)null;

        var activityIds = (await activityRepository.GetAllByUser(effectiveUserId))
            .Where(activity => !IsStageProcessed(activity.ProcessingStatus, jobType))
            .Where(activity => !startDate.HasValue || activity.StartDate >= startDate.Value)
            .Where(activity => !endDate.HasValue || activity.StartDate <= endDate.Value)
            .Select(activity => activity.Id)
            .ToList();

        await QueueActivityCollectionJobs.QueueActivityIdsAsync(activityIds, jobs, queueName, context.RequestAborted);

        return TypedResults.Ok(activityIds.Count);
    }

    private static bool IsStageProcessed(Shared.Models.ActivityProcessingStatus? status, string jobType)
        => jobType.ToLowerInvariant() switch
        {
            "summits" => status?.SummitedPeaks == true,
            "visitedpaths" => status?.VisitedPaths == true,
            "visitedareas" => status?.VisitedAreas == true,
            _ => throw new ArgumentException($"Unsupported job type '{jobType}'", nameof(jobType))
        };
}
