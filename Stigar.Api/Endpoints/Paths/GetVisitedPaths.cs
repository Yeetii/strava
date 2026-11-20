using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Globalization;
using Stigar.Core.Geo;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Shards;

namespace Stigar.Api.Endpoints.Paths;

public class GetVisitedPaths(
    IActivityVisitedPathRepository visitedPathsRepository,
    UserAuthenticationService userAuthService,
    ShardFeatureClient shardFeatureClient)
{
    private sealed record VisitedPathDto(
        string OsmId,
        int TimesVisited,
        string? First,
        string? Last);

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/visitedPaths/{z}/{x}/{y}", (HttpContext context, [FromServices] GetVisitedPaths endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        string? sessionId = context.SessionId();
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
            return TypedResults.Unauthorized();

        if (!TryParseTileCoordinates(context, out var z, out var x, out var y))
            return TypedResults.BadRequest();

        var tileKeys = SlippyTileCalculator.GetIntersectingTileKeys(z, x, y, shardFeatureClient.CanonicalZoom);
        var summaries = await visitedPathsRepository.GetSummaryByUserAndTiles(
            user.Id, tileKeys, context.RequestAborted);

        var result = summaries
            .Select(s => new VisitedPathDto(
                $"way/{s.OsmHighwayId}",
                s.TimesVisited,
                s.FirstDate?.ToString("O"),
                s.LastDate?.ToString("O")))
            .ToArray();
        return TypedResults.Ok(result);
    }

    private static bool TryParseTileCoordinates(HttpContext req, out int z, out int x, out int y)
    {
        z = default;
        x = default;
        y = default;

        var segments = req.Request.Path.Value!.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return false;

        return int.TryParse(segments[^3], NumberStyles.None, CultureInfo.InvariantCulture, out z)
            && int.TryParse(segments[^2], NumberStyles.None, CultureInfo.InvariantCulture, out x)
            && int.TryParse(segments[^1], NumberStyles.None, CultureInfo.InvariantCulture, out y);
    }
}
