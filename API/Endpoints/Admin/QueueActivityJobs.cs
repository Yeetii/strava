using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;

namespace API.Endpoints.Admin;

public class QueueActivityJobs(
    IActivityRepository activityRepository,
    QueueClient queueClient,
    UserAuthenticationService userAuthService)
{
    private static readonly Dictionary<string, string> JobTypeToQueue = new(StringComparer.OrdinalIgnoreCase)
    {
        ["summits"] = Shared.Constants.QueueConfig.CalculateSummitsJobs,
        ["visitedPaths"] = Shared.Constants.QueueConfig.CalculateVisitedPathsJobs,
        ["visitedAreas"] = Shared.Constants.QueueConfig.CalculateVisitedAreasJobs,
    };

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "jobType", In = ParameterLocation.Path, Type = typeof(string), Required = true,
        Description = "Job type: summits | visitedPaths | visitedAreas")]
    [OpenApiParameter(name: "userId", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional target user id. When omitted, queues jobs for the authenticated session user.")]
    [OpenApiParameter(name: "startDate", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional ISO 8601 start date filter (inclusive)")]
    [OpenApiParameter(name: "endDate", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional ISO 8601 end date filter (inclusive)")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of jobs queued.")]
    [Function(nameof(QueueActivityJobs))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/queue/{jobType}")] HttpRequestData req,
        string jobType)
    {
        var response = req.CreateResponse();

        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        if (!JobTypeToQueue.TryGetValue(jobType, out var queueName))
        {
            response.StatusCode = HttpStatusCode.BadRequest;
            await response.WriteStringAsync($"Unknown job type '{jobType}'. Valid values: {string.Join(", ", JobTypeToQueue.Keys)}");
            return response;
        }

        var queryString = System.Web.HttpUtility.ParseQueryString(req.Url.Query);
        var targetUserId = queryString["userId"];
        var effectiveUserId = string.IsNullOrWhiteSpace(targetUserId)
            ? user.Id
            : targetUserId;
        DateTime? startDate = DateTime.TryParse(queryString["startDate"], out var sd) ? sd.ToUniversalTime() : null;
        DateTime? endDate = DateTime.TryParse(queryString["endDate"], out var ed) ? ed.ToUniversalTime() : null;

        var idList = (await activityRepository.GetAllByUser(effectiveUserId))
            .Where(activity => !startDate.HasValue || activity.StartDate >= startDate.Value)
            .Where(activity => !endDate.HasValue || activity.StartDate <= endDate.Value)
            .Select(activity => activity.Id)
            .ToList();

        var sender = queueClient.CreateSender(queueName);
        foreach (var activityId in idList)
        {
            await sender.SendMessageAsync(new QueueMessage(activityId));
        }

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(idList.Count);
        return response;
    }
}
