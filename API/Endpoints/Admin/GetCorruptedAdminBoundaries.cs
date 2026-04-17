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

public class GetCorruptedAdminBoundaries(
    CosmosClient cosmosClient,
    IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(string[]),
        Description = "IDs of admin boundary documents where properties are corrupted (name is an object instead of a string).")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetCorruptedAdminBoundaries))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "manage/adminBoundaries/corrupted")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        // Corrupted docs have OSM tag properties serialized as {"valueKind":3} objects instead of strings.
        // name should always be a string on non-corrupted admin boundaries.
        var query = new QueryDefinition(
            "SELECT c.id FROM c WHERE c.kind = @kind" +
            " AND NOT STARTSWITH(c.id, 'empty-')" +
            " AND NOT STARTSWITH(c.id, 'pointer:')" +
            " AND IS_OBJECT(c.properties.name)")
            .WithParameter("@kind", FeatureKinds.AdminBoundary);

        var ids = new List<string>();
        using var feed = container.GetItemQueryIterator<IdOnly>(query);
        while (feed.HasMoreResults)
        {
            var page = await feed.ReadNextAsync();
            ids.AddRange(page.Select(i => i.Id));
        }

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(ids);
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
