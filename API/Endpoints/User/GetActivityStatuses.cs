using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;

namespace API.Endpoints.User;

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

    [OpenApiOperation(tags: ["Activities"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "limit", In = ParameterLocation.Query, Type = typeof(int), Required = false, Description = "Maximum number of recent activity statuses to return. Defaults to 10, max 100.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<ActivityStatusDto>))]
    [Function(nameof(GetActivityStatuses))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "activityStatuses")] HttpRequestData req)
    {
        var sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await _userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var limit = ParseLimit(req.Query["limit"]);
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

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(statuses);
        return response;
    }

    public static QueryDefinition BuildQuery(string userId, int limit)
    {
        // Cosmos SQL does not support parameterizing TOP; limit is clamped via ParseLimit.
        return new QueryDefinition($"""
            SELECT TOP {limit}
                c.id,
                c.name,
                c.startDateLocal,
                c.summaryPolyline,
                c.processingStatus
            FROM c
            WHERE c.userId = @userId
            ORDER BY c.startDateLocal DESC
            """)
            .WithParameter("@userId", userId);
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
