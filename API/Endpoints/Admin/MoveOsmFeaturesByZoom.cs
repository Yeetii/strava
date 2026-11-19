using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Admin;
using API.Http;

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

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapPost("/api/manage/osmfeatures/moveZoom",
            (HttpContext context, [FromServices] MoveOsmFeaturesByZoom endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var kind = context.Request.Query["kind"].ToString();
        if (string.IsNullOrWhiteSpace(kind))
            return CreateBadRequest("Query parameter 'kind' is required.");

        if (!ValidKinds.Contains(kind))
            return CreateBadRequest($"Unknown kind '{kind}'. Valid values: {string.Join(", ", ValidKinds)}");

        var fromZoomParam = context.Request.Query["fromZoom"].ToString();
        var toZoomParam = context.Request.Query["toZoom"].ToString();
        if (!int.TryParse(fromZoomParam, out var fromZoom))
            return CreateBadRequest("Query parameter 'fromZoom' is required and must be an integer.");
        if (!int.TryParse(toZoomParam, out var toZoom))
            return CreateBadRequest("Query parameter 'toZoom' is required and must be an integer.");
        if (fromZoom == toZoom)
            return CreateBadRequest("Query parameters 'fromZoom' and 'toZoom' must differ.");

        var ids = ParseIds(context.Request.Query["ids"].ToString(), kind);

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

        return TypedResults.Ok(new MoveZoomResult(kind, fromZoom, toZoom, totalScanned, totalMoved, totalSkipped));
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

    private static IResult CreateBadRequest(string message) => TypedResults.BadRequest(message);
}

public record MoveZoomResult(
    string Kind,
    int FromZoom,
    int ToZoom,
    int Scanned,
    int Moved,
    int Skipped);
