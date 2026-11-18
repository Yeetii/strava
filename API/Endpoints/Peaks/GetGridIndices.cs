using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Spatial;

namespace API.Endpoints.Peaks
{
    public class GetGridIndices(IPeakRepository _peaks)
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "peakIds", In = ParameterLocation.Query, Type = typeof(IEnumerable<string>), Required = true)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(IEnumerable<string>), Description = "Grid indices (x,y) containing the peaks")]
        [Function(nameof(GetGridIndices))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "gridIndices")] HttpRequestData req)
        {
            var queryParams = req.Url.ParseQueryString();
            var peakIdsString = queryParams["peakIds"];
            if (peakIdsString == null)
            {
                var badResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                await badResponse.WriteStringAsync("Missing peakIds query parameter");
                return badResponse;
            }
            var peakIds = peakIdsString.Split(',');
            // Match the legacy GetByFeatureIdsAsync lookup: normalise each id and try both
            // the raw and the kind-prefixed form before reading the peaks table.
            var normalizedIds = peakIds
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Select(id => StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, id))
                .Distinct(StringComparer.Ordinal)
                .ToList();
            var candidateIds = normalizedIds
                .SelectMany(id => new[] { id, StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.Peak, id) })
                .Distinct(StringComparer.Ordinal)
                .ToList();
            var peaks = await _peaks.GetByIds(candidateIds);

            var grids = peaks.Select(x => x.X + "," + x.Y).Distinct();

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(grids);
            return response;
        }
    }

}