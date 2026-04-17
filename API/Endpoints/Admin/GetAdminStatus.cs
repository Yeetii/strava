using System.Net;
using Azure.Messaging.ServiceBus.Administration;
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
    IReadOnlyList<CosmosContainerStatus> CosmosContainers);

public class GetAdminStatus(
    ServiceBusAdministrationClient serviceBusAdminClient,
    CosmosClient cosmosClient,
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
        DatabaseConfig.RacesContainer,
        DatabaseConfig.OsmFeaturesContainer,
    ];

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(AdminStatus))]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetAdminStatus))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "admin/status")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
        {
            return req.CreateResponse(HttpStatusCode.Unauthorized);
        }

        var queueTasks = QueueNames.Select(FetchQueueStatusAsync);
        var containerTasks = ContainerNames.Select(FetchContainerStatusAsync);

        var queueStatuses = await Task.WhenAll(queueTasks);
        var containerStatuses = await Task.WhenAll(containerTasks);

        var status = new AdminStatus(queueStatuses, containerStatuses);
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
}
