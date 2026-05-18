using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.User;

public sealed class ActivityStatusProjection
{
    public string Id { get; set; } = string.Empty;
    public string? Name { get; set; }
    public DateTime StartDateLocal { get; set; }
    public string? SummaryPolyline { get; set; }
    public ActivityProcessingStatus? ProcessingStatus { get; set; }
}

public class GetActivityStatuses(
    UserAuthenticationService userAuthService,
    CollectionClient<Activity> activitiesCollection)
{
    private const int DefaultLimit = 10;
    private const int MaxLimit = 100;

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
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var limit = ParseLimit(req.Query["limit"]);
        var statuses = (await activitiesCollection.ExecuteQueryAsync<ActivityStatusProjection>(BuildQuery(user.Id, limit)))
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
