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
    CollectionClient<VisitedPath> visitedPathsCollection,
    CollectionClient<Activity> activityCollection,
    UserAuthenticationService userAuthService)
{
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

        var visitedPaths = await visitedPathsCollection.ExecuteQueryAsync<VisitedPath>(
            new QueryDefinition("SELECT * FROM c WHERE c.userId = @userId").WithParameter("@userId", user.Id)
        );
        var visitedPathsList = visitedPaths.ToList();

        if (visitedPathsList.Count == 0)
        {
            response.StatusCode = HttpStatusCode.OK;
            await response.WriteAsJsonAsync(new FeatureCollection([]));
            return response;
        }

        var pathIds = visitedPathsList.Select(vp => vp.PathId).Distinct().ToList();
        var pathFeatures = (await pathsCollectionClient.GetByIdsAsync(pathIds))
            .ToDictionary(sf => sf.Id, sf => sf.ToFeature());

        var allActivityIds = visitedPathsList.SelectMany(vp => vp.ActivityIds).Distinct().ToArray();
        var activitiesById = allActivityIds.Length == 0
            ? new Dictionary<string, Activity>()
            : (await activityCollection.GetByIdsAsync(allActivityIds)).ToDictionary(a => a.Id, a => a);

        var features = visitedPathsList
            .Where(vp => pathFeatures.ContainsKey(vp.PathId))
            .Select(vp =>
            {
                var feature = pathFeatures[vp.PathId];
                feature.Properties["timesVisited"] = vp.ActivityIds.Count;
                feature.Properties["activityIds"] = vp.ActivityIds.Order().ToArray();

                var sortedDates = vp.ActivityIds
                    .Select(activityId => activitiesById.TryGetValue(activityId, out var activity)
                        ? activity.StartDateLocal
                        : (DateTime?)null)
                    .Where(d => d.HasValue)
                    .Select(d => d!.Value)
                    .OrderBy(d => d)
                    .Select(d => d.ToString("O"))
                    .ToArray();

                feature.Properties["visitedDates"] = sortedDates;
                if (sortedDates.Length > 0)
                {
                    feature.Properties["firstVisited"] = sortedDates[0];
                    feature.Properties["lastVisited"] = sortedDates[^1];
                }

                return feature;
            })
            .OrderByDescending(f => f.Properties.TryGetValue("timesVisited", out var timesVisited) ? (int)timesVisited : 0)
            .ToList();

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(new FeatureCollection(features));
        return response;
    }
}
