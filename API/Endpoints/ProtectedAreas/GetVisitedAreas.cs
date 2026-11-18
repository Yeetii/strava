using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Spatial;

namespace API.Endpoints.ProtectedAreas;

public class GetVisitedAreas(
    IActivityVisitedAreaRepository visitedAreasRepository,
    IProtectedAreaRepository protectedAreasRepository,
    IActivityRepository activityRepository,
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
        Description = "Optional comma-separated filter by area type (e.g. national_park,nature_reserve,protected_area,region,country).")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<VisitedAreaDto>),
        Description = "A list of areas the authenticated user has visited, including visit counts and dates.")]
    [Function(nameof(GetVisitedAreas))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "visitedAreas")] HttpRequestData req)
    {
        var response = req.CreateResponse();
        string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
        {
            response.StatusCode = HttpStatusCode.Unauthorized;
            return response;
        }

        var areaType = req.Query["areaType"];
        var areaTypes = areaType?
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(value => value.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var links = await visitedAreasRepository.GetByUser(user.Id);
        var areaIds = links
            .Select(link => StoredFeature.NormalizeFeatureId(FeatureKinds.ProtectedArea, link.AreaId))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        var lookupIds = areaIds
            .SelectMany(id => new[] { id, StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.ProtectedArea, id) })
            .Distinct(StringComparer.Ordinal);
        var areas = await protectedAreasRepository.GetByIds(lookupIds);
        var areaById = areas.ToDictionary(
            area => area.LogicalId,
            area => area,
            StringComparer.OrdinalIgnoreCase);

        var allActivityIds = links.Select(link => link.ActivityId).Distinct().ToArray();
        var activitiesById = allActivityIds.Length == 0
            ? []
            : (await activityRepository.GetByIds(allActivityIds)).ToDictionary(a => a.Id, a => a);

        var result = links
            .GroupBy(link => StoredFeature.NormalizeFeatureId(FeatureKinds.ProtectedArea, link.AreaId))
            .Where(group => areaById.ContainsKey(group.Key))
            .Select(group =>
            {
                var area = areaById[group.Key];
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

                return new VisitedAreaDto(
                    group.Key,
                    GetText(area.Properties, "name") ?? string.Empty,
                    GetText(area.Properties, "areaType") ?? string.Empty,
                    activityIds.Length,
                    activityIds.Order().ToArray(),
                    sortedDates,
                    GetText(area.Properties, "wikidata"),
                    GetText(area.Properties, "wikimediaCommons"));
            })
            .Where(area => areaTypes is not { Length: > 0 }
                || areaTypes.Contains(area.AreaType.ToLowerInvariant()))
            .OrderByDescending(area => area.TimesVisited)
            .ThenBy(area => area.Name)
            .ToArray();

        response.StatusCode = HttpStatusCode.OK;
        await response.WriteAsJsonAsync(result);
        return response;
    }

    private static string? GetText(IDictionary<string, dynamic> properties, string key)
        => properties.TryGetValue(key, out var value) && value is not null
            ? value.ToString()
            : null;
}
