using System.Net;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Queue;

public class QueueActivityJobs(
    CollectionClient<Activity> activityCollection,
    ServiceBusClient serviceBusClient,
    UserAuthenticationService userAuthService)
{
    private static readonly Dictionary<string, string> JobTypeToQueue = new(StringComparer.OrdinalIgnoreCase)
    {
        ["summits"] = Shared.Constants.ServiceBusConfig.CalculateSummitsJobs,
        ["visitedPaths"] = Shared.Constants.ServiceBusConfig.CalculateVisitedPathsJobs,
        ["visitedAreas"] = Shared.Constants.ServiceBusConfig.CalculateVisitedAreasJobs,
    };

    [OpenApiOperation(tags: ["Queue"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "jobType", In = ParameterLocation.Path, Type = typeof(string), Required = true,
        Description = "Job type: summits | visitedPaths | visitedAreas")]
    [OpenApiParameter(name: "startDate", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional ISO 8601 start date filter (inclusive)")]
    [OpenApiParameter(name: "endDate", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional ISO 8601 end date filter (inclusive)")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of jobs queued.")]
    [Function(nameof(QueueActivityJobs))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "queue/{jobType}")] HttpRequestData req,
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
        DateTime? startDate = DateTime.TryParse(queryString["startDate"], out var sd) ? sd.ToUniversalTime() : null;
        DateTime? endDate = DateTime.TryParse(queryString["endDate"], out var ed) ? ed.ToUniversalTime() : null;

        var queryBuilder = new System.Text.StringBuilder("SELECT VALUE c.id FROM c WHERE c.userId = @userId");
        var queryDefinition = new QueryDefinition(queryBuilder.ToString());

        if (startDate.HasValue || endDate.HasValue)
        {
            queryBuilder.Clear();
            queryBuilder.Append("SELECT VALUE c.id FROM c WHERE c.userId = @userId");
            if (startDate.HasValue)
                queryBuilder.Append(" AND c.startDate >= @startDate");
            if (endDate.HasValue)
                queryBuilder.Append(" AND c.startDate <= @endDate");

            queryDefinition = new QueryDefinition(queryBuilder.ToString())
                .WithParameter("@userId", user.Id);

            if (startDate.HasValue)
                queryDefinition = queryDefinition.WithParameter("@startDate", startDate.Value.ToString("O"));
            if (endDate.HasValue)
                queryDefinition = queryDefinition.WithParameter("@endDate", endDate.Value.ToString("O"));
        }
        else
        {
            queryDefinition = queryDefinition.WithParameter("@userId", user.Id);
        }

        var activityIds = await activityCollection.ExecuteQueryAsync<string>(queryDefinition);
        var idList = activityIds.ToList();

        var sender = serviceBusClient.CreateSender(queueName);
        foreach (var activityId in idList)
        {
            await sender.SendMessageAsync(new ServiceBusMessage(activityId));
        }

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(idList.Count);
        return response;
    }
}
