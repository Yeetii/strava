using System.Net;
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
    IConfiguration configuration,
    ILogger<ReEnrichAdminBoundaries> logger)
{
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
            "SELECT c.id, c.x, c.y FROM c WHERE c.kind = @kind AND NOT STARTSWITH(c.id, 'empty-') AND NOT STARTSWITH(c.id, 'pointer:')")
            .WithParameter("@kind", FeatureKinds.AdminBoundary);

        var items = new List<(string Id, int X, int Y)>();
        using (var feedIterator = container.GetItemQueryIterator<BoundaryKey>(queryDefinition))
        {
            while (feedIterator.HasMoreResults)
            {
                var page = await feedIterator.ReadNextAsync();
                items.AddRange(page.Select(i => (i.Id, i.X, i.Y)));
            }
        }

        logger.LogInformation("Resetting adminBoundaryMetricsVersion on {Count} admin boundaries to trigger re-enrichment", items.Count);

        const int batchSize = 5;
        for (int i = 0; i < items.Count; i += batchSize)
        {
            var batch = items.Skip(i).Take(batchSize);
            var patchTasks = batch.Select(item =>
            {
                var partitionKey = new PartitionKeyBuilder().Add(item.X).Add(item.Y).Build();
                return container.PatchItemAsync<object>(
                    item.Id,
                    partitionKey,
                    [PatchOperation.Set("/properties/adminBoundaryMetricsVersion", 0)]);
            });
            await Task.WhenAll(patchTasks);
            await Task.Delay(200);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(items.Count);
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

    private record BoundaryKey(string Id, int X, int Y);
}
