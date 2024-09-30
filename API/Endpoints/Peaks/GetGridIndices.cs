using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API.Endpoints.Peaks
{
    public class GetGridIndices(PeaksCollectionClient _peaksCollection)
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
            var peaks = await _peaksCollection.GetByIdsAsync(peakIds);

            var grids = peaks.Select(x => x.X + "," + x.Y).Distinct();

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(grids);
            return response;
        }
    }

}