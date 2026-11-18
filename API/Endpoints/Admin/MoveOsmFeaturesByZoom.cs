using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Admin;

public class MoveOsmFeaturesByZoom(
    IOsmFeatureAdminQueries adminQueries,
    IConfiguration configuration,
    ILogger<MoveOsmFeaturesByZoom> logger)
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
    [OpenApiParameter(name: "kind", In = ParameterLocation.Query, Type = typeof(string), Required = true,
        Description = "Feature kind to move: peak | path | protectedArea | adminBoundary | race")]
    [OpenApiParameter(name: "fromZoom", In = ParameterLocation.Query, Type = typeof(int), Required = true,
        Description = "Source storage zoom level for rows to move.")]
    [OpenApiParameter(name: "toZoom", In = ParameterLocation.Query, Type = typeof(int), Required = true,
        Description = "Destination zoom level for the recomputed tile_x/tile_y.")]
    [OpenApiParameter(name: "ids", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional comma-separated feature IDs to move. IDs may be provided with or without the kind prefix.")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(MoveZoomResult),
        Description = "Summary of retiled rows.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(MoveOsmFeaturesByZoom))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "manage/osmfeatures/moveZoom")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        if (!IsAuthorized(req))
            return req.CreateResponse(HttpStatusCode.Unauthorized);

        var kind = req.Query["kind"];
        if (string.IsNullOrWhiteSpace(kind))
            return await CreateBadRequest(req, "Query parameter 'kind' is required.");

        if (!ValidKinds.Contains(kind))
            return await CreateBadRequest(req, $"Unknown kind '{kind}'. Valid values: {string.Join(", ", ValidKinds)}");

        var fromZoomParam = req.Query["fromZoom"];
        var toZoomParam = req.Query["toZoom"];
        if (!int.TryParse(fromZoomParam, out var fromZoom))
            return await CreateBadRequest(req, "Query parameter 'fromZoom' is required and must be an integer.");
        if (!int.TryParse(toZoomParam, out var toZoom))
            return await CreateBadRequest(req, "Query parameter 'toZoom' is required and must be an integer.");
        if (fromZoom == toZoom)
            return await CreateBadRequest(req, "Query parameters 'fromZoom' and 'toZoom' must differ.");

        var ids = ParseIds(req.Query["ids"], kind);

        var totalScanned = 0;
        var totalMoved = 0;
        if (TryGetRetileableTable(kind, out var table))
        {
            var result = await adminQueries.RetileOsmFeaturesAsync(
                table, fromZoom, toZoom, ids, cancellationToken);
            totalScanned = result.Scanned;
            totalMoved = result.Moved;
        }
        else
        {
            // admin_boundaries has no tile columns; paths and races have no domain table.
            logger.LogWarning(
                "Kind '{Kind}' cannot be retiled in PostgreSQL (no tile_x/tile_y columns or no domain table)",
                kind);
        }

        var totalSkipped = totalScanned - totalMoved;
        logger.LogInformation(
            "Retiled {TotalMoved} rows for kind {Kind} from zoom {FromZoom} to zoom {ToZoom}. Scanned={TotalScanned}, Skipped={TotalSkipped}.",
            totalMoved,
            kind,
            fromZoom,
            toZoom,
            totalScanned,
            totalSkipped);

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(new MoveZoomResult(kind, fromZoom, toZoom, totalScanned, totalMoved, totalSkipped));
        return response;
    }

    private static bool TryGetRetileableTable(string kind, out OsmFeatureTable table)
    {
        switch (kind.ToLowerInvariant())
        {
            case FeatureKinds.Peak:
                table = OsmFeatureTable.Peaks;
                return true;
            case FeatureKinds.ProtectedArea:
                table = OsmFeatureTable.ProtectedAreas;
                return true;
            default:
                table = default;
                return false;
        }
    }

    private static HashSet<string> ParseIds(string? idsParam, string kind)
    {
        if (string.IsNullOrWhiteSpace(idsParam))
            return new HashSet<string>(StringComparer.Ordinal);

        return idsParam
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(id => StoredFeature.NormalizeFeatureId(kind, id))
            .Select(id => StoredFeature.EnsurePrefixedFeatureId(kind, id))
            .ToHashSet(StringComparer.Ordinal);
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

public record MoveZoomResult(
    string Kind,
    int FromZoom,
    int ToZoom,
    int Scanned,
    int Moved,
    int Skipped);
