using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using System.Net;
using BAMCIS.GeoJSON;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Api
{
    public class GetAllSummitedPeaks(
        IActivitySummitRepository summitsRepository,
        IPeakRepository peaksRepository,
        IActivityRepository activityRepository,
        UserAuthenticationService userAuthService)
    {
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/summitedPeaks", (HttpContext context, [FromServices] GetAllSummitedPeaks endpoint) => endpoint.Handle(context));

        public async Task<IResult> Handle(HttpContext context)
        {
            string? sessionId = context.SessionId();

            var user = await userAuthService.GetUserFromSessionId(sessionId);
            if (user == default)
                return TypedResults.Unauthorized();

            var links = await summitsRepository.GetByUser(user.Id);
            var summitGroups = links
                .GroupBy(link => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, link.PeakId))
                .ToDictionary(group => group.Key, group => group.ToArray(), StringComparer.OrdinalIgnoreCase);
            var lookupIds = summitGroups.Keys
                .SelectMany(id => new[] { id, StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.Peak, id) })
                .Distinct(StringComparer.Ordinal);
            var peaks = await peaksRepository.GetByIds(lookupIds);

            var allActivityIds = links.Select(link => link.ActivityId).Distinct().ToArray();
            var activitiesById = allActivityIds.Length == 0
                ? new Dictionary<string, Activity>()
                : (await activityRepository.GetByIds(allActivityIds)).ToDictionary(activity => activity.Id, activity => activity);

            var features = peaks.Select(peak =>
            {
                var feature = peak.ToFeature();
                summitGroups.TryGetValue(peak.LogicalId, out var summitLinks);
                var activityIds = summitLinks?.Select(link => link.ActivityId).Distinct().ToArray() ?? [];
                var ascentDates = activityIds
                    .Select(activityId => activitiesById.TryGetValue(activityId, out var activity)
                        ? activity.StartDateLocal
                        : (DateTime?)null)
                    .Where(date => date.HasValue)
                    .Select(date => date!.Value)
                    .OrderBy(date => date)
                    .ToArray();

                feature.Properties["summited"] = true;
                feature.Properties["summitsCount"] = activityIds.Length;
                if (ascentDates.Length > 0)
                {
                    feature.Properties["firstAscent"] = ascentDates.First().ToString("O");
                    feature.Properties["lastAscent"] = ascentDates.Last().ToString("O");
                }
                return feature;
            }).ToList();

            var featureCollection = new FeatureCollection(features);

            return TypedResults.Ok(featureCollection);
        }
    }
}
