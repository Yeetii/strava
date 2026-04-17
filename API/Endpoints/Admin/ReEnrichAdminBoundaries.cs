using System.Net;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Services;

namespace API.Endpoints.Admin;

public class ReEnrichAdminBoundaries(
    CosmosClient cosmosClient,
    ServiceBusClient serviceBusClient,
    IConfiguration configuration,
    ILogger<ReEnrichAdminBoundaries> logger)
{
    private readonly ServiceBusSender _sender = serviceBusClient.CreateSender(ServiceBusConfig.EnrichAdminBoundaryJobs);

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of admin boundary documents queued for re-enrichment.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(ReEnrichAdminBoundaries))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/adminBoundaries/reEnrich")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        var queryDefinition = new QueryDefinition(
            "SELECT c.id FROM c WHERE c.kind = @kind AND NOT STARTSWITH(c.id, 'empty-') AND NOT STARTSWITH(c.id, 'pointer:')")
            .WithParameter("@kind", FeatureKinds.AdminBoundary);

        var ids = new List<string>();
        using (var feedIterator = container.GetItemQueryIterator<IdOnly>(queryDefinition))
        {
            while (feedIterator.HasMoreResults)
            {
                var page = await feedIterator.ReadNextAsync();
                ids.AddRange(page.Select(i => i.Id));
            }
        }

        logger.LogInformation("Queuing {Count} admin boundary enrichment jobs", ids.Count);

        const int batchSize = 100;
        for (int i = 0; i < ids.Count; i += batchSize)
        {
            var messages = ids.Skip(i).Take(batchSize).Select(id => new ServiceBusMessage(id)).ToList();
            await _sender.SendMessagesAsync(messages);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(ids.Count);
        return response;
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;

        return req.Headers.TryGetValues("x-admin-key", out var providedKeys)
            && providedKeys.FirstOrDefault() == adminKey;
    }

    private record IdOnly(string Id);
}
