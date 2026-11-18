using System.Globalization;
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Spatial;

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
        IActivitySummitRepository summitsRepository,
        IPeakRepository peaksRepository)
    {
        [OpenApiOperation(tags: ["Aggregates"])]
        [OpenApiParameter(name: "session", In = ParameterLocation.Cookie, Type = typeof(string), Required = false)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(SummitsStats))]
        [Function(nameof(GetSummitsStats))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "summitsStats")] HttpRequestData req)
        {
            var response = req.CreateResponse();
            string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;

            var user = await _userAuthService.GetUserFromSessionId(sessionId);
            if (user == default)
            {
                response.StatusCode = HttpStatusCode.Unauthorized;
                return response;
            }

            var linksByPeak = (await summitsRepository.GetByUser(user.Id))
                .GroupBy(link => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, link.PeakId))
                .ToDictionary(group => group.Key, group => group.ToArray(), StringComparer.OrdinalIgnoreCase);
            var lookupIds = linksByPeak.Keys
                .SelectMany(id => new[] { id, StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.Peak, id) })
                .Distinct(StringComparer.Ordinal);
            var peaks = await peaksRepository.GetByIds(lookupIds);

            var summitedPeaks = peaks.Select(peak =>
            {
                linksByPeak.TryGetValue(peak.LogicalId, out var summitLinks);
                return new SummitedPeak
                {
                    Id = $"{user.Id}-{peak.LogicalId}",
                    Name = GetText(peak.Properties, "name") ?? string.Empty,
                    UserId = user.Id,
                    PeakId = peak.LogicalId,
                    Elevation = ParseElevation(peak.Properties),
                    ActivityIds = summitLinks?.Select(link => link.ActivityId).Distinct().ToHashSet()
                        ?? new HashSet<string>()
                };
            }).ToList();

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

        private static string? GetText(IDictionary<string, dynamic> properties, string key)
            => properties.TryGetValue(key, out var value) && value is not null
                ? value.ToString()
                : null;

        private static float? ParseElevation(IDictionary<string, dynamic> properties)
        {
            var raw = GetText(properties, "ele") ?? GetText(properties, "elevation");
            return float.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var elevation)
                ? elevation
                : null;
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
