using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Aggregates
{
    public class VisitedPeak
    {
        public required string Id { get; set; }
        public required string Name { get; set; }
        public required int Count { get; set; }
        public float? Elevation { get; set; }
    }

    public class SummitsStats
    {
        public int TotalPeaksClimbed { get; set; }
        public required int[] TotalPeaksClimbedCategorized { get; set; }
        public required VisitedPeak[] MostVisitedPeaks { get; set; }

    }

    public class GetSummitsStats(UserAuthenticationService _userAuthService,
        CollectionClient<SummitedPeak> _summitedPeaksCollection)
    {
        [OpenApiOperation(tags: ["Aggregates"])]
        [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = false)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(SummitsStats))]
        [Function(nameof(GetSummitsStats))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "summitsStats")] HttpRequestData req)
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
            var summitedPeaks = (await _summitedPeaksCollection.ExecuteQueryAsync<SummitedPeak>(summitedPeaksQuery)).ToList();

            var summitsStats = new SummitsStats
            {
                TotalPeaksClimbed = summitedPeaks.Count,
                TotalPeaksClimbedCategorized = GetTotalPeaksClimbedCategorized(summitedPeaks),
                MostVisitedPeaks = GetMostVisitedPeaks(summitedPeaks)
            };

            response.StatusCode = HttpStatusCode.OK;
            await response.WriteAsJsonAsync(summitsStats);
            return response;
        }

        private static int[] GetTotalPeaksClimbedCategorized(IEnumerable<SummitedPeak> summitedPeaks)
        {
            int[] totalPeaksClimbedCategorized = [0, 0, 0, 0, 0, 0, 0, 0, 0];
            foreach (var summitedPeak in summitedPeaks)
            {
                if (summitedPeak.Elevation is null)
                    continue;
                int category = (int)summitedPeak.Elevation / 1000;
                totalPeaksClimbedCategorized[category]++;
            }
            return totalPeaksClimbedCategorized;
        }

        private static VisitedPeak[] GetMostVisitedPeaks(IEnumerable<SummitedPeak> summitedPeaks)
        {
            return summitedPeaks.Select(peak => new VisitedPeak
            {
                Id = peak.Id,
                Name = peak.Name,
                Elevation = peak.Elevation ?? 0,
                Count = peak.ActivityIds.Count
            })
            .OrderByDescending(visitedPeak => visitedPeak.Count)
            .Take(5).ToArray();
        }
    }
}
