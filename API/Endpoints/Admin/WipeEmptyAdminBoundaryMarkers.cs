using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Services;

namespace API.Endpoints.Admin;

public class WipeEmptyAdminBoundaryMarkers(
    CosmosClient cosmosClient,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "adminLevel", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "OSM admin_level to wipe empty markers for (e.g. 2 or 4). When omitted, all admin levels are wiped.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of empty markers deleted.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(WipeEmptyAdminBoundaryMarkers))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "manage/adminBoundaries/emptyMarkers")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        var adminLevelParam = req.Query["adminLevel"];
        QueryDefinition query;

        if (!string.IsNullOrWhiteSpace(adminLevelParam) && int.TryParse(adminLevelParam, out var adminLevel))
        {
            query = new QueryDefinition(
                "SELECT c.id, c.x, c.y FROM c" +
                " WHERE c.kind = @kind" +
                " AND STARTSWITH(c.id, @prefix)")
                .WithParameter("@kind", FeatureKinds.AdminBoundary)
                .WithParameter("@prefix", $"empty-{FeatureKinds.AdminBoundary}-{adminLevel}-");
        }
        else
        {
            query = new QueryDefinition(
                "SELECT c.id, c.x, c.y FROM c" +
                " WHERE c.kind = @kind" +
                " AND STARTSWITH(c.id, @prefix)")
                .WithParameter("@kind", FeatureKinds.AdminBoundary)
                .WithParameter("@prefix", $"empty-{FeatureKinds.AdminBoundary}-");
        }

        var toDelete = new List<(string Id, double X, double Y)>();
        using var feed = container.GetItemQueryIterator<CoordItem>(query);
        while (feed.HasMoreResults)
        {
            var page = await feed.ReadNextAsync();
            toDelete.AddRange(page.Select(i => (i.Id, (double)i.X, (double)i.Y)));
        }

        foreach (var (id, x, y) in toDelete)
        {
            var pk = new PartitionKeyBuilder().Add(x).Add(y).Build();
            await container.DeleteItemAsync<object>(id, pk);
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(toDelete.Count);
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

    private record CoordItem(string Id, int X, int Y);
}
