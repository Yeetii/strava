using System.Net;
using System.Text;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Services;

namespace API.Endpoints.Admin;

public class QueueUnprocessedActivityJobs(
    ServiceBusClient serviceBusClient,
    UserAuthenticationService userAuthService,
    CollectionClient<Shared.Models.Activity> activityCollection)
{
    private static readonly Dictionary<string, string> JobTypeToQueue = new(StringComparer.OrdinalIgnoreCase)
    {
        ["summits"] = ServiceBusConfig.CalculateSummitsJobs,
        ["visitedPaths"] = ServiceBusConfig.CalculateVisitedPathsJobs,
        ["visitedAreas"] = ServiceBusConfig.CalculateVisitedAreasJobs,
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
        Description = "Number of unprocessed jobs queued.")]
    [Function(nameof(QueueUnprocessedActivityJobs))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/queue/unprocessed/{jobType}")] HttpRequestData req,
        string jobType)
    {
        var response = req.CreateResponse();

        var sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == null)
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
        var effectiveUserId = string.IsNullOrWhiteSpace(targetUserId) ? user.Id : targetUserId;
        var startDate = DateTime.TryParse(queryString["startDate"], out var sd) ? sd.ToUniversalTime() : (DateTime?)null;
        var endDate = DateTime.TryParse(queryString["endDate"], out var ed) ? ed.ToUniversalTime() : (DateTime?)null;

        var query = BuildQuery(effectiveUserId, jobType, startDate, endDate);
        var activityIds = (await activityCollection.ExecuteQueryAsync<string>(query)).ToList();

        var sender = serviceBusClient.CreateSender(queueName);
        foreach (var activityId in activityIds)
            await sender.SendMessageAsync(new ServiceBusMessage(activityId));

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(activityIds.Count);
        return response;
    }

    internal static QueryDefinition BuildQuery(string userId, string jobType, DateTime? startDate, DateTime? endDate)
    {
        var queryBuilder = new StringBuilder("SELECT VALUE c.id FROM c WHERE c.userId = @userId");

        queryBuilder.Append(jobType.ToLowerInvariant() switch
        {
            "summits" => " AND (NOT IS_DEFINED(c.processingStatus.summitedPeaks) OR c.processingStatus.summitedPeaks = false)",
            "visitedpaths" => " AND (NOT IS_DEFINED(c.processingStatus.visitedPaths) OR c.processingStatus.visitedPaths = false)",
            "visitedareas" => " AND (NOT IS_DEFINED(c.processingStatus.visitedAreas) OR c.processingStatus.visitedAreas = false)",
            _ => throw new ArgumentException($"Unsupported job type '{jobType}'", nameof(jobType))
        });

        if (startDate.HasValue)
            queryBuilder.Append(" AND c.startDate >= @startDate");
        if (endDate.HasValue)
            queryBuilder.Append(" AND c.startDate <= @endDate");

        var query = new QueryDefinition(queryBuilder.ToString())
            .WithParameter("@userId", userId);

        if (startDate.HasValue)
            query = query.WithParameter("@startDate", startDate.Value.ToString("O"));
        if (endDate.HasValue)
            query = query.WithParameter("@endDate", endDate.Value.ToString("O"));

        return query;
    }
}