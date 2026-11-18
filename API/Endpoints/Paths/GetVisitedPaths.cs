using System.Globalization;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
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

    [OpenApiOperation(tags: ["Paths"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "z", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<VisitedPathDto>),
        Description = "A list of paths the authenticated user has been on within the requested tile.")]
    [Function(nameof(GetVisitedPaths))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "visitedPaths/{z}/{x}/{y}")] HttpRequestData req)
    {
        var response = req.CreateResponse();

        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        if (!TryParseTileCoordinates(req, out var z, out var x, out var y))
        {
            response.StatusCode = HttpStatusCode.BadRequest;
            return response;
        }

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

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(result);
        return response;
    }

    private static bool TryParseTileCoordinates(HttpRequestData req, out int z, out int x, out int y)
    {
        z = default;
        x = default;
        y = default;

        var segments = req.Url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return false;

        return int.TryParse(segments[^3], NumberStyles.None, CultureInfo.InvariantCulture, out z)
            && int.TryParse(segments[^2], NumberStyles.None, CultureInfo.InvariantCulture, out x)
            && int.TryParse(segments[^1], NumberStyles.None, CultureInfo.InvariantCulture, out y);
    }
}
