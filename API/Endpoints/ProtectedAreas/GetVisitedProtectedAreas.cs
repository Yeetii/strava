using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Geo;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.ProtectedAreas;

public class GetVisitedProtectedAreas(
    ProtectedAreasCollectionClient protectedAreasCollectionClient,
    CollectionClient<Activity> activityCollection,
    UserAuthenticationService userAuthService)
{
    private const int ProtectedAreaTileZoom = 8;

    private sealed record VisitedProtectedAreaDto(
        string AreaId,
        string Name,
        string AreaType,
        int TimesVisited,
        string[] ActivityIds,
        string[] Dates,
        string? Wikidata,
        string? WikimediaCommons);

    [OpenApiOperation(tags: ["ProtectedAreas"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<VisitedProtectedAreaDto>),
        Description = "A list of protected areas the authenticated user has been in, including visit counts and dates.")]
    [Function(nameof(GetVisitedProtectedAreas))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "visitedProtectedAreas")] HttpRequestData req)
    {
        var response = req.CreateResponse();

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
                ProtectedAreaTileZoom))
            .Distinct()
            .ToList();

        var protectedAreas = (await protectedAreasCollectionClient.FetchByTiles(tileIndices, ProtectedAreaTileZoom))
            .Select(area => area.ToFeature())
            .ToDictionary(feature => feature.Id.Value, feature => feature);

        var visitedAreas = new Dictionary<string, HashSet<string>>();
        foreach (var activity in activityList)
        {
            var polyline = activity.Polyline ?? activity.SummaryPolyline ?? string.Empty;
            foreach (var area in protectedAreas.Values)
            {
                if (RouteFeatureMatcher.RouteIntersectsPolygon(polyline, area.Geometry))
                {
                    if (!visitedAreas.TryGetValue(area.Id.Value, out var activityIds))
                    {
                        activityIds = [];
                        visitedAreas[area.Id.Value] = activityIds;
                    }

                    activityIds.Add(activity.Id);
                }
            }
        }

        var activitiesById = activityList.ToDictionary(activity => activity.Id, activity => activity);
        var result = visitedAreas
            .Select(entry =>
            {
                var area = protectedAreas[entry.Key];
                var sortedDates = entry.Value
                    .Select(activityId => activitiesById[activityId].StartDateLocal)
                    .OrderBy(date => date)
                    .Select(date => date.ToString("O"))
                    .ToArray();

                return new VisitedProtectedAreaDto(
                    entry.Key,
                    area.Properties.TryGetValue("name", out var name) ? name?.ToString() ?? entry.Key : entry.Key,
                    area.Properties.TryGetValue("areaType", out var areaType) ? areaType?.ToString() ?? "protected_area" : "protected_area",
                    entry.Value.Count,
                    entry.Value.Order().ToArray(),
                    sortedDates,
                    area.Properties.TryGetValue("wikidata", out var wikidata) ? wikidata?.ToString() : null,
                    area.Properties.TryGetValue("wikimedia_commons", out var wikimediaCommons) ? wikimediaCommons?.ToString() : null
                );
            })
            .OrderByDescending(area => area.TimesVisited)
            .ThenBy(area => area.Name)
            .ToArray();

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(result);
        return response;
    }
}