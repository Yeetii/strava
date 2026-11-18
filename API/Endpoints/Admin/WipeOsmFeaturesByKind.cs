using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Services;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Admin;

public class WipeOsmFeaturesByKind(
    IOsmFeatureAdminQueries adminQueries,
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
        Description = "Feature kind to wipe: peak | path | protectedArea | adminBoundary | race. path and race have no PostgreSQL domain table.")]
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "Optional storage zoom to filter by. The domain tables have fixed storage zooms, so a mismatched value deletes nothing.")]
    [OpenApiParameter(name: "ids", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional comma-separated row IDs to wipe. When omitted, all rows of the given kind are wiped.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(int),
        Description = "Number of rows deleted.")]
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

        if (!TryGetTable(kind, out var table))
        {
            var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
            await badRequest.WriteStringAsync(
                $"Kind '{kind}' has no PostgreSQL domain table; there is nothing to wipe.");
            return badRequest;
        }

        var zoomParam = req.Query["zoom"];
        int? zoom = null;
        if (!string.IsNullOrWhiteSpace(zoomParam))
        {
            if (!int.TryParse(zoomParam, out var parsedZoom))
                return await CreateBadRequest(req, "Query parameter 'zoom' must be an integer.");
            zoom = parsedZoom;
        }

        var idsParam = req.Query["ids"];
        string[]? ids = null;
        if (!string.IsNullOrWhiteSpace(idsParam))
        {
            ids = idsParam
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToArray();
        }

        var deleted = await adminQueries.DeleteOsmFeaturesAsync(table, zoom, ids);
        logger.LogInformation("Deleted {Count} osmFeatures rows of kind '{Kind}'", deleted, kind);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(deleted);
        return response;
    }

    private static bool TryGetTable(string kind, out OsmFeatureTable table)
    {
        switch (kind.ToLowerInvariant())
        {
            case FeatureKinds.Peak:
                table = OsmFeatureTable.Peaks;
                return true;
            case FeatureKinds.ProtectedArea:
                table = OsmFeatureTable.ProtectedAreas;
                return true;
            case FeatureKinds.AdminBoundary:
                table = OsmFeatureTable.AdminBoundaries;
                return true;
            default:
                table = default;
                return false;
        }
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
}
