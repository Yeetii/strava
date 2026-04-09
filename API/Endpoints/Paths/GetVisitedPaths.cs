using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Paths;

public class GetVisitedPaths(
    CollectionClient<VisitedPath> visitedPathsCollection,
    CollectionClient<Activity> activityCollection,
    UserAuthenticationService userAuthService)
{
    private sealed record VisitedPathDto(
        string PathId,
        string? Name,
        string? Type,
        int TimesVisited,
        string[] ActivityIds,
        string[] Dates);

    [OpenApiOperation(tags: ["Paths"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<VisitedPathDto>),
        Description = "A list of paths the authenticated user has been on.")]
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

                return new VisitedPathDto(
                    vp.PathId,
                    vp.Name,
                    vp.Type,
                    vp.ActivityIds.Count,
                    vp.ActivityIds.Order().ToArray(),
                    sortedDates
                );
            })
            .OrderByDescending(p => p.TimesVisited)
            .ToArray();

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(result);
        return response;
    }
}
