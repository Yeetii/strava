using System.Collections.Immutable;
using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Microsoft.Extensions.DependencyInjection;
using Shared.Services;
using API.Utils;

namespace API
{
    public class GetAllSummitedPeaks(
        [FromKeyedServices(FeatureKinds.Peak)] TiledCollectionClient _peaksCollection,
        CollectionClient<SummitedPeak> _summitedPeakCollection,
        CollectionClient<Activity> _activityCollection,
        UserAuthenticationService _userAuthService)
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = true)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
            Description = "A GeoJson FeatureCollection with peaks the user has summited.")]
        [Function(nameof(GetAllSummitedPeaks))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "summitedPeaks")] HttpRequestData req)
        {
            var response = req.CreateResponse();
            // Probably won't need this in production if I move the API to the same domain as the frontend
            response.Headers.Add("Access-Control-Allow-Credentials", "true");
            string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;

            var user = await _userAuthService.GetUserFromSessionId(sessionId);
            if (user == default)
            {
                response.StatusCode = HttpStatusCode.Unauthorized;
                return response;
            }

            var summitedPeaksQuery = new QueryDefinition($"SELECT * FROM c where c.userId = '{user.Id}'");
            var summitedPeaks = SummitedPeakConsolidator.ConsolidateByPeakId(
                await _summitedPeakCollection.ExecuteQueryAsync<SummitedPeak>(summitedPeaksQuery)
            );
            var summitedPeaksDict = summitedPeaks
                .ToDictionary(g => g.PeakId, g => g);
            var summitedPeaksIds = summitedPeaks.Select(x => x.PeakId).ToImmutableHashSet();
            var activityIds = summitedPeaks
                .SelectMany(x => x.ActivityIds)
                .Distinct()
                .ToArray();
            var activitiesById = activityIds.Length == 0
                ? new Dictionary<string, Activity>()
                : (await _activityCollection.GetByIdsAsync(activityIds)).ToDictionary(activity => activity.Id, activity => activity);

            var peaks = await _peaksCollection.GetByFeatureIdsAsync(summitedPeaksIds);

            var features = peaks.Select(x =>
            {
                var feature = x.ToFeature();
                var summitedPeak = summitedPeaksDict[x.LogicalId];
                var ascentDates = summitedPeak.ActivityIds
                    .Select(activityId => activitiesById.TryGetValue(activityId, out var activity)
                        ? activity.StartDateLocal
                        : (DateTime?)null)
                    .Where(date => date.HasValue)
                    .Select(date => date!.Value)
                    .OrderBy(date => date)
                    .ToArray();
                // Add summit-specific properties
                feature.Properties["summited"] = true;
                feature.Properties["summitsCount"] = summitedPeak.ActivityIds.Count;
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
