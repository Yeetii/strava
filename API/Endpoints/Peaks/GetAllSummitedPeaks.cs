using System.Collections.Immutable;
using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API
{
    public class GetAllSummitedPeaks(CollectionClient<StoredFeature> _peaksCollection,
        CollectionClient<SummitedPeak> _summitedPeakCollection,
        CollectionClient<Shared.Models.User> _usersCollection,
        UserAuthenticationService _userAuthService)
    {
        const int DefaultZoom = 11;

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
            var summitedPeaks = await _summitedPeakCollection.ExecuteQueryAsync<SummitedPeak>(summitedPeaksQuery);
            var summitedPeaksIds = summitedPeaks.Select(x => x.PeakId).ToImmutableHashSet();

            var peaks = await _peaksCollection.GetByIdsAsync(summitedPeaksIds);

            var featureCollection = new FeatureCollection
            {
                Features = peaks.Select(x =>
                {
                    var p = x.ToFeature();
                    p.Properties.Add("summited", true.ToString());
                    return p;
                })
            };

            response.StatusCode = HttpStatusCode.OK;
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }
    }
}
