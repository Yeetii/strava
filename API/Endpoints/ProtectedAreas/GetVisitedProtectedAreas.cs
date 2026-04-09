using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.ProtectedAreas;

public class GetVisitedProtectedAreas(
    CollectionClient<VisitedArea> visitedAreasCollection,
    CollectionClient<Activity> activityCollection,
    UserAuthenticationService userAuthService)
{
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
        response.Headers.Add("Access-Control-Allow-Credentials", "true");
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        var visitedAreas = await visitedAreasCollection.ExecuteQueryAsync<VisitedArea>(
            new QueryDefinition("SELECT * FROM c WHERE c.userId = @userId").WithParameter("@userId", user.Id)
        );
        var visitedAreasList = visitedAreas.ToList();

        var allActivityIds = visitedAreasList.SelectMany(va => va.ActivityIds).Distinct().ToArray();
        var activitiesById = allActivityIds.Length == 0
            ? new Dictionary<string, Activity>()
            : (await activityCollection.GetByIdsAsync(allActivityIds)).ToDictionary(a => a.Id, a => a);

        var result = visitedAreasList
            .Select(va =>
            {
                var sortedDates = va.ActivityIds
                    .Select(activityId => activitiesById.TryGetValue(activityId, out var activity)
                        ? activity.StartDateLocal
                        : (DateTime?)null)
                    .Where(d => d.HasValue)
                    .Select(d => d!.Value)
                    .OrderBy(d => d)
                    .Select(d => d.ToString("O"))
                    .ToArray();

                return new VisitedProtectedAreaDto(
                    va.AreaId,
                    va.Name,
                    va.AreaType,
                    va.ActivityIds.Count,
                    va.ActivityIds.Order().ToArray(),
                    sortedDates,
                    va.Wikidata,
                    va.WikimediaCommons
                );
            })
            .OrderByDescending(a => a.TimesVisited)
            .ThenBy(a => a.Name)
            .ToArray();

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(result);
        return response;
    }
}
