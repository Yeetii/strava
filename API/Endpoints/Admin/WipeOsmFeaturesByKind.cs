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

public class WipeOsmFeaturesByKind(
    CosmosClient cosmosClient,
    IConfiguration configuration,
    ILogger<WipeOsmFeaturesByKind> logger)
{
    private static readonly HashSet<string> ValidKinds = new(StringComparer.OrdinalIgnoreCase)
    {
        FeatureKinds.Peak,
        FeatureKinds.Path,
        FeatureKinds.ProtectedArea,
        FeatureKinds.AdminBoundary,
        FeatureKinds.Race,
    };

    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "kind", In = ParameterLocation.Path, Type = typeof(string), Required = true,
        Description = "Feature kind to wipe: peak | path | protectedArea | adminBoundary | race")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of documents marked for expiry (ttl=1).")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [Function(nameof(WipeOsmFeaturesByKind))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "manage/osmfeatures/{kind}")] HttpRequestData req,
        string kind)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        if (!ValidKinds.Contains(kind))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            await badRequest.WriteStringAsync(
                $"Unknown kind '{kind}'. Valid values: {string.Join(", ", ValidKinds)}");
            return badRequest;
        }

        var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        var queryDefinition = new QueryDefinition("SELECT c.id, c.x, c.y FROM c WHERE c.kind = @kind")
            .WithParameter("@kind", kind);

        var items = new List<(string Id, int X, int Y)>();
        using (var feedIterator = container.GetItemQueryIterator<OsmFeatureKey>(queryDefinition))
        {
            while (feedIterator.HasMoreResults)
            {
                var page = await feedIterator.ReadNextAsync();
                items.AddRange(page.Select(i => (i.Id, i.X, i.Y)));
            }
        }

        logger.LogInformation(
            "Setting ttl=1 on {Count} osmFeatures documents of kind '{Kind}'", items.Count, kind);

        const int batchSize = 10;
        for (int i = 0; i < items.Count; i += batchSize)
        {
            var batch = items.Skip(i).Take(batchSize);
            var patchTasks = batch.Select(item =>
            {
                var partitionKey = new PartitionKeyBuilder().Add(item.X).Add(item.Y).Build();
                return container.PatchItemAsync<object>(
                    item.Id,
                    partitionKey,
                    [PatchOperation.Set("/ttl", 1)]);
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

    private record OsmFeatureKey(string Id, int X, int Y);
}
