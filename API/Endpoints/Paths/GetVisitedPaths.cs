using Microsoft.AspNetCore.Mvc;
using API.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Globalization;
using System.Net;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Spatial;
using Shared.Services.Shards;

namespace API.Endpoints.Paths;

public class GetVisitedPaths(
    IActivityVisitedPathRepository visitedPathsRepository,
    IActivityRepository activityRepository,
    UserAuthenticationService userAuthService,
    ShardFeatureClient shardFeatureClient)
{
    private sealed record VisitedPathDto(
        string? OsmId,
        string? Name,
        string? Type,
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
        var links = (await visitedPathsRepository.GetByUser(user.Id))
            .Where(link => link.TileX.HasValue && link.TileY.HasValue
                && tileKeys.Any(tile => tile.x == link.TileX.Value && tile.y == link.TileY.Value))
            .ToArray();

        var allActivityIds = links.Select(link => link.ActivityId).Distinct().ToArray();
        var activitiesById = allActivityIds.Length == 0
            ? new Dictionary<string, Activity>()
            : (await activityRepository.GetByIds(allActivityIds)).ToDictionary(a => a.Id, a => a);

        var result = links
            .GroupBy(link => link.OsmHighwayId)
            .Select(group =>
            {
                var activityIds = group.Select(link => link.ActivityId).Distinct().ToArray();
                var sortedDates = activityIds
                    .Select(activityId => activitiesById.TryGetValue(activityId, out var activity)
                        ? activity.StartDateLocal
                        : (DateTime?)null)
                    .Where(date => date.HasValue)
                    .Select(date => date!.Value)
                    .OrderBy(date => date)
                    .Select(date => date.ToString("O"))
                    .ToArray();

                return new VisitedPathDto(
                    group.Key,
                    group.Select(link => link.Name).FirstOrDefault(name => !string.IsNullOrWhiteSpace(name)),
                    group.Select(link => link.Type).FirstOrDefault(type => !string.IsNullOrWhiteSpace(type)),
                    activityIds.Length,
                    sortedDates.FirstOrDefault(),
                    sortedDates.LastOrDefault());
            })
            .OrderByDescending(path => path.TimesVisited)
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
