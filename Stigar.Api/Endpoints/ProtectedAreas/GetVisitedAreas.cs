using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Api.Endpoints.ProtectedAreas;

public class GetVisitedAreas(
    IActivityVisitedAreaRepository visitedAreasRepository,
    IProtectedAreaRepository protectedAreasRepository,
    IAdminBoundaryRepository adminBoundariesRepository,
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
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/visitedAreas", (HttpContext context, [FromServices] GetVisitedAreas endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        string? sessionId = context.SessionId();
        var user = await userAuthService.GetUserFromSessionId(sessionId);
        if (user == default)
            return TypedResults.Unauthorized();

        string? areaType = context.Request.Query["areaType"].ToString();
        var areaTypes = areaType?
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(value => value.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var links = await visitedAreasRepository.GetByUser(user.Id);
        var rawAreaIds = links.Select(link => link.AreaId).Distinct(StringComparer.Ordinal).ToArray();

        var protectedLookupIds = rawAreaIds
            .Where(id => !id.StartsWith("country:", StringComparison.OrdinalIgnoreCase))
            .SelectMany(id => new[] {
                id,
                StoredFeature.NormalizeFeatureId(FeatureKinds.ProtectedArea, id),
                StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.ProtectedArea, id)
            })
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var protectedAreas = await protectedAreasRepository.GetByIds(protectedLookupIds);
        var protectedById = protectedAreas.ToDictionary(
            area => area.LogicalId,
            area => area,
            StringComparer.OrdinalIgnoreCase);

        var remainingIds = rawAreaIds
            .Where(id => !id.StartsWith("country:", StringComparison.OrdinalIgnoreCase) && !protectedById.ContainsKey(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        var adminBoundaries = remainingIds.Length > 0
            ? await adminBoundariesRepository.GetByIds(remainingIds)
            : Array.Empty<StoredFeature>();
        var adminById = adminBoundaries.ToDictionary(
            boundary => boundary.LogicalId,
            boundary => boundary,
            StringComparer.OrdinalIgnoreCase);

        var allActivityIds = links.Select(link => link.ActivityId).Distinct().ToArray();
        var activitiesById = allActivityIds.Length == 0
            ? []
            : (await activityRepository.GetByIds(allActivityIds)).ToDictionary(a => a.Id, a => a);

        var result = links
            .GroupBy(link => link.AreaId)
            .Select(group =>
            {
                var areaId = group.Key;
                string name;
                string resolvedAreaType;
                string? wikidata = null;
                string? wikimediaCommons = null;

                if (areaId.StartsWith("country:", StringComparison.OrdinalIgnoreCase))
                {
                    var countryCode = areaId["country:".Length..].Trim().ToUpperInvariant();
                    name = GetCountryName(countryCode);
                    resolvedAreaType = "country";
                }
                else if (protectedById.TryGetValue(areaId, out var prot) ||
                         protectedById.TryGetValue(StoredFeature.NormalizeFeatureId(FeatureKinds.ProtectedArea, areaId), out prot))
                {
                    name = GetText(prot.Properties, "name") ?? prot.LogicalId;
                    resolvedAreaType = GetText(prot.Properties, "areaType") ?? "protected_area";
                    wikidata = GetText(prot.Properties, "wikidata");
                    wikimediaCommons = GetText(prot.Properties, "wikimediaCommons") ?? GetText(prot.Properties, "wikimedia_commons");
                }
                else if (adminById.TryGetValue(areaId, out var admin))
                {
                    name = GetText(admin.Properties, "name") ?? admin.LogicalId;
                    resolvedAreaType = "region";
                    wikidata = GetText(admin.Properties, "wikidata");
                    wikimediaCommons = GetText(admin.Properties, "wikimediaCommons") ?? GetText(admin.Properties, "wikimedia_commons");
                }
                else
                {
                    name = areaId;
                    resolvedAreaType = "unknown";
                }

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
                    areaId,
                    name,
                    resolvedAreaType,
                    activityIds.Length,
                    activityIds.Order().ToArray(),
                    sortedDates,
                    wikidata,
                    wikimediaCommons);
            })
            .Where(area => areaTypes is not { Length: > 0 }
                || areaTypes.Contains(area.AreaType.ToLowerInvariant()))
            .OrderByDescending(area => area.TimesVisited)
            .ThenBy(area => area.Name)
            .ToArray();
        return TypedResults.Ok(result);
    }

    private static string GetCountryName(string iso2)
    {
        try
        {
            return new System.Globalization.RegionInfo(iso2).EnglishName;
        }
        catch
        {
            return iso2;
        }
    }

    private static string? GetText(IDictionary<string, dynamic> properties, string key)
        => properties.TryGetValue(key, out var value) && value is not null
            ? value.ToString()
            : null;
}
