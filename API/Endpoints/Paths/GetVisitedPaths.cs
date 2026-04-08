using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Geo;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Paths;

public class GetVisitedPaths(
    PathsCollectionClient pathsCollectionClient,
    CollectionClient<Activity> activityCollection,
    UserAuthenticationService userAuthService)
{
    private const int PathTileZoom = 11;

    [OpenApiOperation(tags: ["Paths"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
        Description = "A GeoJson FeatureCollection of paths the authenticated user has been on.")]
    [Function(nameof(GetVisitedPaths))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "visitedPaths")] HttpRequestData req)
    {
        var response = req.CreateResponse();
        response.Headers.Add("Access-Control-Allow-Credentials", "true");

        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        var activities = await activityCollection.ExecuteQueryAsync<Activity>(
            new QueryDefinition("SELECT * FROM c WHERE c.userId = @userId").WithParameter("@userId", user.Id)
        );

        var activityList = activities
            .Where(activity => !string.IsNullOrWhiteSpace(activity.Polyline ?? activity.SummaryPolyline))
            .ToList();

        var tileIndices = activityList
            .SelectMany(activity => SlippyTileCalculator.TileIndicesByLine(
                GeoSpatialFunctions.DecodePolyline(activity.Polyline ?? activity.SummaryPolyline ?? string.Empty),
                PathTileZoom))
            .Distinct()
            .ToList();

        var pathFeatures = (await pathsCollectionClient.FetchByTiles(tileIndices, PathTileZoom)).Features
            .ToDictionary(feature => feature.Id.Value, feature => feature);

        var visitedPathActivityIds = new Dictionary<string, HashSet<string>>();
        foreach (var activity in activityList)
        {
            var polyline = activity.Polyline ?? activity.SummaryPolyline ?? string.Empty;
            foreach (var path in pathFeatures.Values)
            {
                if (path.Geometry is not LineString line)
                {
                    continue;
                }

                var lineCoordinates = line.Coordinates.Select(position => new Coordinate(position.Longitude, position.Latitude));
                if (RouteFeatureMatcher.RouteIntersectsLine(polyline, lineCoordinates))
                {
                    if (!visitedPathActivityIds.TryGetValue(path.Id.Value, out var activityIds))
                    {
                        activityIds = [];
                        visitedPathActivityIds[path.Id.Value] = activityIds;
                    }

                    activityIds.Add(activity.Id);
                }
            }
        }

        var activitiesById = activityList.ToDictionary(activity => activity.Id, activity => activity);
        var features = visitedPathActivityIds
            .Select(entry =>
            {
                var feature = pathFeatures[entry.Key];
                feature.Properties["timesVisited"] = entry.Value.Count;
                feature.Properties["activityIds"] = entry.Value.Order().ToArray();

                var sortedDates = entry.Value
                    .Select(activityId => activitiesById[activityId].StartDateLocal)
                    .OrderBy(date => date)
                    .Select(date => date.ToString("O"))
                    .ToArray();

                feature.Properties["visitedDates"] = sortedDates;
                if (sortedDates.Length > 0)
                {
                    feature.Properties["firstVisited"] = sortedDates[0];
                    feature.Properties["lastVisited"] = sortedDates[^1];
                }

                return feature;
            })
            .OrderByDescending(feature => feature.Properties.TryGetValue("timesVisited", out var timesVisited) ? (int)timesVisited : 0)
            .ToList();

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(new FeatureCollection(features));
        return response;
    }
}