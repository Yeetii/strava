using System.Net;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Monitor.Query;
using Azure.Monitor.Query.Models;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Constants;

namespace API.Endpoints.Admin;

public record ServiceBusQueueStatus(
    string QueueName,
    long ActiveMessageCount,
    long ScheduledMessageCount,
    long DeadLetterMessageCount);

public record CosmosContainerStatus(
    string ContainerName,
    long DocumentCount);

public record AdminStatus(
    IReadOnlyList<ServiceBusQueueStatus> ServiceBusQueues,
    IReadOnlyList<CosmosContainerStatus> CosmosContainers,
    int? ProvisionedThroughput,
    double? LiveRuPerSecond,
    int NewActivitiesLast24Hours,
    int ProcessedNewActivitiesLast24Hours);

internal sealed record ActivityWindowCountsProjection(
    int NewActivitiesLast24Hours,
    int ProcessedNewActivitiesLast24Hours);

public class GetAdminStatus(
    ServiceBusAdministrationClient serviceBusAdminClient,
    CosmosClient cosmosClient,
    MetricsQueryClient metricsQueryClient,
    IConfiguration configuration,
    ILogger<GetAdminStatus> logger)
{
    private static readonly string[] QueueNames =
    [
        ServiceBusConfig.CalculateSummitsJobs,
        ServiceBusConfig.CalculateVisitedPathsJobs,
        ServiceBusConfig.CalculateVisitedAreasJobs,
        ServiceBusConfig.ActivitiesFetchJobs,
        ServiceBusConfig.ActivityFetchJobs,
        ServiceBusConfig.ActivityProcessed,
        ServiceBusConfig.ScrapeRace,
    ];

    private static readonly string[] ContainerNames =
    [
        DatabaseConfig.SummitedPeaksContainer,
        DatabaseConfig.ActivitiesContainer,
        DatabaseConfig.UsersContainer,
        DatabaseConfig.UserSyncItemsContainer,
        DatabaseConfig.SessionsContainer,
        DatabaseConfig.VisitedPathsContainer,
        DatabaseConfig.VisitedAreasContainer,
        DatabaseConfig.PeaksGroupsContainer,
        DatabaseConfig.OsmFeaturesContainer,
    ];

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(AdminStatus))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetAdminStatus))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/status")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
        {
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        var queueTasks = QueueNames.Select(FetchQueueStatusAsync);
        var containerTasks = ContainerNames.Select(FetchContainerStatusAsync);
        var throughputTask = FetchDatabaseThroughputAsync();
        var liveRuTask = FetchLiveRuPerSecondAsync();
        var activityWindowCountsTask = FetchActivityWindowCountsAsync();

        var queueStatuses = await Task.WhenAll(queueTasks);
        var containerStatuses = await Task.WhenAll(containerTasks);
        var provisionedThroughput = await throughputTask;
        var liveRuPerSecond = await liveRuTask;
        var activityWindowCounts = await activityWindowCountsTask;

        var status = new AdminStatus(
            queueStatuses,
            containerStatuses,
            provisionedThroughput,
            liveRuPerSecond,
            activityWindowCounts.NewActivitiesLast24Hours,
            activityWindowCounts.ProcessedNewActivitiesLast24Hours);
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(status);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
        {
            return false;
        }

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }

    private async Task<ServiceBusQueueStatus> FetchQueueStatusAsync(string queueName)
    {
        try
        {
            var props = await serviceBusAdminClient.GetQueueRuntimePropertiesAsync(queueName);
            return new ServiceBusQueueStatus(
                queueName,
                props.Value.ActiveMessageCount,
                props.Value.ScheduledMessageCount,
                props.Value.DeadLetterMessageCount);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch Service Bus runtime properties for queue {QueueName}", queueName);
            return new ServiceBusQueueStatus(queueName, -1, -1, -1);
        }
    }

    private async Task<CosmosContainerStatus> FetchContainerStatusAsync(string containerName)
    {
        try
        {
            var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, containerName);
            var query = new QueryDefinition("SELECT VALUE COUNT(1) FROM c");
            using var iterator = container.GetItemQueryIterator<long>(query);
            if (iterator.HasMoreResults)
            {
                var response = await iterator.ReadNextAsync();
                return new CosmosContainerStatus(containerName, response.FirstOrDefault());
            }

            return new CosmosContainerStatus(containerName, 0);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch document count for Cosmos container {ContainerName}", containerName);
            return new CosmosContainerStatus(containerName, -1);
        }
    }

    private async Task<int?> FetchDatabaseThroughputAsync()
    {
        try
        {
            return await cosmosClient.GetDatabase(DatabaseConfig.CosmosDb).ReadThroughputAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch Cosmos database throughput");
            return null;
        }
    }

    private async Task<ActivityWindowCountsProjection> FetchActivityWindowCountsAsync()
    {
        try
        {
            var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer);
            var sinceUtc = DateTime.UtcNow.AddHours(-24);
            var newActivitiesTask = FetchScalarCountAsync(
                container,
                new QueryDefinition(
                    "SELECT VALUE COUNT(1) FROM c WHERE c.startDateLocal >= @sinceUtc")
                    .WithParameter("@sinceUtc", sinceUtc));

            var processedNewActivitiesTask = FetchScalarCountAsync(
                container,
                new QueryDefinition(
                    """
                    SELECT VALUE COUNT(1)
                    FROM c
                    WHERE c.startDateLocal >= @sinceUtc
                      AND IS_DEFINED(c.processingStatus)
                      AND c.processingStatus.summitedPeaks = true
                      AND c.processingStatus.visitedPaths = true
                      AND c.processingStatus.visitedAreas = true
                    """)
                    .WithParameter("@sinceUtc", sinceUtc));

            await Task.WhenAll(newActivitiesTask, processedNewActivitiesTask);

            return new ActivityWindowCountsProjection(
                newActivitiesTask.Result,
                processedNewActivitiesTask.Result);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch 24-hour activity window counts");
            return new ActivityWindowCountsProjection(-1, -1);
        }
    }

    private static async Task<int> FetchScalarCountAsync(Container container, QueryDefinition query)
    {
        using var iterator = container.GetItemQueryIterator<int>(query);
        if (!iterator.HasMoreResults)
        {
            return 0;
        }

        var response = await iterator.ReadNextAsync();
        return response.FirstOrDefault();
    }

    private async Task<double?> FetchLiveRuPerSecondAsync()
    {
        var resourceId = configuration.GetValue<string>("CosmosAccountResourceId");
        if (string.IsNullOrEmpty(resourceId))
        {
            return null;
        }

        try
        {
            // Query TotalRequestUnits over the last 5 minutes with 1-minute granularity.
            // The metric reports RUs consumed per minute; dividing by 60 gives RU/s.
            var options = new MetricsQueryOptions
            {
                Granularity = TimeSpan.FromMinutes(1),
                TimeRange = new QueryTimeRange(TimeSpan.FromMinutes(5))
            };
            options.Aggregations.Add(MetricAggregationType.Total);

            var result = await metricsQueryClient.QueryResourceAsync(
                resourceId,
                ["TotalRequestUnits"],
                options);

            var timeSeries = result.Value.Metrics.FirstOrDefault()?.TimeSeries.FirstOrDefault();
            if (timeSeries is null)
            {
                return null;
            }

            // Take the most recent data point that has a value
            var latest = timeSeries.Values
                .Where(v => v.Total.HasValue)
                .OrderByDescending(v => v.TimeStamp)
                .FirstOrDefault();

            return latest?.Total / 60.0;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to fetch live Cosmos RU/s from Azure Monitor");
            return null;
        }
    }
}
