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
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "Optional tile zoom level to filter documents by.")]
    [OpenApiParameter(name: "ids", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional comma-separated document IDs to wipe. When omitted, all documents of the given kind are wiped.")]
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

        var idsParam = req.Query["ids"];
        List<(string Id, int X, int Y)> items;

        var zoomParam = req.Query["zoom"];
        int? zoom = null;
        if (!string.IsNullOrWhiteSpace(zoomParam))
        {
            if (!int.TryParse(zoomParam, out var parsedZoom))
                return await CreateBadRequest(req, "Query parameter 'zoom' must be an integer.");
            zoom = parsedZoom;
        }

        if (!string.IsNullOrWhiteSpace(idsParam))
        {
            var requestedIds = idsParam
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToHashSet(StringComparer.Ordinal);

            var inClause = string.Join(",", requestedIds.Select((_, i) => $"@id{i}"));
            var queryText = "SELECT c.id, c.x, c.y FROM c WHERE c.kind = @kind AND c.id IN (" + inClause + ")";
            if (zoom.HasValue)
                queryText += " AND c.zoom = @zoom";

            var queryDefinition = new QueryDefinition(queryText)
                .WithParameter("@kind", kind);
            if (zoom.HasValue)
                queryDefinition = queryDefinition.WithParameter("@zoom", zoom.Value);

            int idx = 0;
            foreach (var id in requestedIds)
                queryDefinition.WithParameter($"@id{idx++}", id);

            items = [];
            using var feedIterator = container.GetItemQueryIterator<OsmFeatureKey>(queryDefinition);
            while (feedIterator.HasMoreResults)
            {
                var page = await feedIterator.ReadNextAsync();
                items.AddRange(page.Select(i => (i.Id, i.X, i.Y)));
            }
        }
        else
        {
            var queryText = "SELECT c.id, c.x, c.y FROM c WHERE c.kind = @kind";
            if (zoom.HasValue)
                queryText += " AND c.zoom = @zoom";

            var queryDefinition = new QueryDefinition(queryText)
                .WithParameter("@kind", kind);
            if (zoom.HasValue)
                queryDefinition = queryDefinition.WithParameter("@zoom", zoom.Value);

            items = [];
            using var feedIterator = container.GetItemQueryIterator<OsmFeatureKey>(queryDefinition);
            while (feedIterator.HasMoreResults)
            {
                var page = await feedIterator.ReadNextAsync();
                items.AddRange(page.Select(i => (i.Id, i.X, i.Y)));
            }
        }

        logger.LogInformation(
            "Setting ttl=1 on {Count} osmFeatures documents of kind '{Kind}'", items.Count, kind);

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

    private static async Task<HttpResponseData> CreateBadRequest(HttpRequestData req, string message)
    {
        var response = req.CreateResponse(HttpStatusCode.BadRequest);
        await response.WriteStringAsync(message);
        return response;
    }

    private record OsmFeatureKey(string Id, int X, int Y);
}
