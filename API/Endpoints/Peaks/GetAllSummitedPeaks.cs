using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Spatial;

namespace API
{
    public class GetAllSummitedPeaks(
        IActivitySummitRepository summitsRepository,
        IPeakRepository peaksRepository,
        IActivityRepository activityRepository,
        UserAuthenticationService userAuthService)
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
            Description = "A GeoJson FeatureCollection with peaks the user has summited.")]
        [Function(nameof(GetAllSummitedPeaks))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "summitedPeaks")] HttpRequestData req)
        {
            var response = req.CreateResponse();
            string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;

            var user = await userAuthService.GetUserFromSessionId(sessionId);
            if (user == default)
            {
                response.StatusCode = HttpStatusCode.Unauthorized;
                return response;
            }

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

            response.StatusCode = HttpStatusCode.OK;
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }
    }
}
