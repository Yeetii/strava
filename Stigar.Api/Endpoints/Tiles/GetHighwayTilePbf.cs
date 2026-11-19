using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using Stigar.Api.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Core.Services.Shards;
using System.Globalization;
using System.Diagnostics;

namespace Stigar.Api.Endpoints.Tiles;

public class GetHighwayTilePbf(BlobTileService blobTileService, IConfiguration configuration, ILogger<GetHighwayTilePbf> logger)
{
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/tiles/highways/{z}/{x}/{y}.pbf",
            (HttpContext context, [FromServices] GetHighwayTilePbf endpoint) => endpoint.Handle(context, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
    {
        var start = Stopwatch.StartNew();
        var invocationId = context.TraceIdentifier;
        int z = default;
        int x = default;
        int y = default;
        var forceRefresh = false;

        try
        {
            if (!TryParseTileCoordinates(context, out z, out x, out y))
                return TypedResults.BadRequest();

            forceRefresh = string.Equals(context.Request.Query["forceRefresh"], "true", StringComparison.OrdinalIgnoreCase);
            if (forceRefresh && !context.IsAdminAuthorized(configuration))
                return TypedResults.Unauthorized();

            var tile = forceRefresh
                ? await blobTileService.RefreshTileAsync(z, x, y, cancellationToken)
                : await blobTileService.BuildTileAsync(z, x, y, cancellationToken);

            logger.LogInformation(
                "GetHighwayTilePbf succeeded for z{Z}/{X}/{Y} (forceRefresh={ForceRefresh}, invocationId={InvocationId}) in {ElapsedMs}ms",
                z,
                x,
                y,
                forceRefresh,
                invocationId,
                start.ElapsedMilliseconds);

            context.Response.Headers["Content-Type"] = "application/x-protobuf";
            context.Response.Headers["Content-Encoding"] = "gzip";
            if (tile.SourceWrittenAt is not null)
                context.Response.Headers["X-Tile-Source-Written-At"] = tile.SourceWrittenAt.Value.ToString("O");
            context.Response.Headers["Cache-Control"] = forceRefresh || !tile.IsComplete ? "no-store" : "public,max-age=60";
            if (!forceRefresh && !tile.IsComplete)
                context.Response.Headers["X-Tile-Incomplete"] = "true";
            await context.Response.Body.WriteAsync(tile.Payload, cancellationToken);
            return TypedResults.Empty;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            logger.LogWarning(
                ex,
                "GetHighwayTilePbf cancelled for z{Z}/{X}/{Y} (forceRefresh={ForceRefresh}, invocationId={InvocationId}, elapsedMs={ElapsedMs}, cancellationRequested={CancellationRequested})",
                z,
                x,
                y,
                forceRefresh,
                invocationId,
                start.ElapsedMilliseconds,
                cancellationToken.IsCancellationRequested);
            return TypedResults.NoContent();
        }
    }


    private static bool TryParseTileCoordinates(HttpContext req, out int z, out int x, out int y)
    {
        z = default;
        x = default;
        y = default;

        var segments = req.Request.Path.Value!.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return false;

        var ySegment = segments[^1];
        if (!ySegment.EndsWith(".pbf", StringComparison.OrdinalIgnoreCase))
            return false;

        return int.TryParse(segments[^3], NumberStyles.None, CultureInfo.InvariantCulture, out z)
            && int.TryParse(segments[^2], NumberStyles.None, CultureInfo.InvariantCulture, out x)
            && int.TryParse(ySegment[..^4], NumberStyles.None, CultureInfo.InvariantCulture, out y);
    }
}
