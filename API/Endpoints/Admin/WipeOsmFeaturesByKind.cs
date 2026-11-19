using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared.Services.Postgres.Admin;
using API.Http;

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

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapDelete("/api/manage/osmfeatures/{kind}",
            (HttpContext context, [FromServices] WipeOsmFeaturesByKind endpoint, string kind) => endpoint.Handle(context, kind));

    public async Task<IResult> Handle(HttpContext context, string kind)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        if (!ValidKinds.Contains(kind))
            return TypedResults.BadRequest(
                $"Unknown kind '{kind}'. Valid values: {string.Join(", ", ValidKinds)}");

        if (!TryGetTable(kind, out var table))
            return TypedResults.BadRequest(
                $"Kind '{kind}' has no PostgreSQL domain table; there is nothing to wipe.");

        var zoomParam = context.Request.Query["zoom"].ToString();
        int? zoom = null;
        if (!string.IsNullOrWhiteSpace(zoomParam))
        {
            if (!int.TryParse(zoomParam, out var parsedZoom))
                return CreateBadRequest("Query parameter 'zoom' must be an integer.");
            zoom = parsedZoom;
        }

        var idsParam = context.Request.Query["ids"].ToString();
        string[]? ids = null;
        if (!string.IsNullOrWhiteSpace(idsParam))
        {
            ids = idsParam
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToArray();
        }

        var deleted = await adminQueries.DeleteOsmFeaturesAsync(table, zoom, ids);
        logger.LogInformation("Deleted {Count} osmFeatures rows of kind '{Kind}'", deleted, kind);

        return TypedResults.Ok(deleted);
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

    private static IResult CreateBadRequest(string message) => TypedResults.BadRequest(message);
}
