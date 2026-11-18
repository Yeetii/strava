using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Admin;

public class WipeEmptyAdminBoundaryMarkers(
    IOsmFeatureAdminQueries adminQueries,
    IConfiguration configuration,
    ILogger<WipeEmptyAdminBoundaryMarkers> logger)
{
    [OpenApiOperation(tags: ["Admin"])]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "adminLevel", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "Accepted for compatibility. PostgreSQL tile-query markers are keyed by kind/zoom/x/y and carry no admin level, so all adminBoundary markers are wiped.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of adminBoundary tile-query markers deleted.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(WipeEmptyAdminBoundaryMarkers))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "delete", Route = "manage/adminBoundaries/emptyMarkers")] HttpRequestData req)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var adminLevelParam = req.Query["adminLevel"];
        if (!string.IsNullOrWhiteSpace(adminLevelParam) && int.TryParse(adminLevelParam, out var adminLevel))
        {
            logger.LogWarning(
                "adminLevel={AdminLevel} cannot narrow the wipe: osm_tile_queries has no admin_level column; all adminBoundary tile-query markers will be deleted",
                adminLevel);
        }

        var deleted = await adminQueries.DeleteOsmTileQueriesAsync();

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(deleted);
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
}
