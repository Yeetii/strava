using System.Globalization;
using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using Shared.Services.Shards;

namespace API.Endpoints.Paths;

public class GetVisitedPaths(
    CollectionClient<VisitedPath> visitedPathsCollection,
    CollectionClient<Activity> activityCollection,
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

        List<VisitedPath> visitedPathsList;
        if (tileKeys.Count == 1)
        {
            var (tileX, tileY) = tileKeys[0];
            visitedPathsList = (await visitedPathsCollection.ExecuteQueryAsync<VisitedPath>(
                new QueryDefinition("SELECT * FROM c WHERE c.userId = @userId AND c.tileX = @tileX AND c.tileY = @tileY")
                    .WithParameter("@userId", user.Id)
                    .WithParameter("@tileX", tileX)
                    .WithParameter("@tileY", tileY)
            )).ToList();
        }
        else
        {
            // Low zoom: collect results from all intersecting z12 tiles
            var results = new List<VisitedPath>();
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (tileX, tileY) in tileKeys)
            {
                var page = await visitedPathsCollection.ExecuteQueryAsync<VisitedPath>(
                    new QueryDefinition("SELECT * FROM c WHERE c.userId = @userId AND c.tileX = @tileX AND c.tileY = @tileY")
                        .WithParameter("@userId", user.Id)
                        .WithParameter("@tileX", tileX)
                        .WithParameter("@tileY", tileY)
                );
                foreach (var vp in page)
                    if (seen.Add(vp.Id))
                        results.Add(vp);
            }
            visitedPathsList = results;
        }

        var allActivityIds = visitedPathsList.SelectMany(vp => vp.ActivityIds).Distinct().ToArray();
        var activitiesById = allActivityIds.Length == 0
            ? new Dictionary<string, Activity>()
            : (await activityCollection.GetByIdsAsync(allActivityIds)).ToDictionary(a => a.Id, a => a);

        var result = visitedPathsList
            .Select(vp =>
            {
                var sortedDates = vp.ActivityIds
                    .Select(activityId => activitiesById.TryGetValue(activityId, out var activity)
                        ? activity.StartDateLocal
                        : (DateTime?)null)
                    .Where(d => d.HasValue)
                    .Select(d => d!.Value)
                    .OrderBy(d => d)
                    .Select(d => d.ToString("O"))
                    .ToArray();

                var first = sortedDates.FirstOrDefault();
                var last = sortedDates.LastOrDefault();

                return new VisitedPathDto(
                    vp.OsmHighwayId,
                    vp.Name,
                    vp.Type,
                    vp.ActivityIds.Count,
                    first,
                    last
                );
            })
            .OrderByDescending(p => p.TimesVisited)
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

