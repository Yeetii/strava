using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.ProtectedAreas;

public class GetVisitedAreas(
    CollectionClient<VisitedArea> visitedAreasCollection,
    CollectionClient<Activity> activityCollection,
    UserAuthenticationService userAuthService)
{
    private sealed record VisitedAreaDto(
        string AreaId,
        string Name,
        string AreaType,
        int TimesVisited,
        string[] ActivityIds,
        string[] Dates,
        string? Wikidata,
        string? WikimediaCommons);

    [OpenApiOperation(tags: ["VisitedAreas"])]
    [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
    [OpenApiParameter(name: "areaType", In = ParameterLocation.Query, Type = typeof(string), Required = false,
        Description = "Optional comma-separated filter by area type (e.g. national_park,nature_reserve,protected_area,region).")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<VisitedAreaDto>),
        Description = "A list of areas the authenticated user has visited, including visit counts and dates.")]
    [Function(nameof(GetVisitedAreas))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "visitedAreas")] HttpRequestData req)
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

        var areaType = req.Query["areaType"];
        var areaTypes = areaType?.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        QueryDefinition query;
        if (areaTypes is { Length: > 0 })
        {
            var inClause = string.Join(",", areaTypes.Select((_, i) => $"@areaType{i}"));
            var qd = new QueryDefinition($"SELECT * FROM c WHERE c.userId = @userId AND c.areaType IN ({inClause})")
                .WithParameter("@userId", user.Id);
            for (int i = 0; i < areaTypes.Length; i++)
                qd = qd.WithParameter($"@areaType{i}", areaTypes[i]);
            query = qd;
        }
        else
        {
            query = new QueryDefinition("SELECT * FROM c WHERE c.userId = @userId")
                .WithParameter("@userId", user.Id);
        }

        var visitedAreas = await visitedAreasCollection.ExecuteQueryAsync<VisitedArea>(query);
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

                return new VisitedAreaDto(
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
