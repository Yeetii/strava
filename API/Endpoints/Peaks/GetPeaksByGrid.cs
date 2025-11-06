using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.Peaks
{
    public class GetPeaksByGrid(PeaksCollectionClient _peaksCollection)
    {
        const int DefaultZoom = 11;

        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(double), Required = false)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
            Description = "A GeoJson FeatureCollection with peaks.")]
        [Function(nameof(GetPeaksByGrid))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "peaks/{x}/{y}")] HttpRequestData req, int x, int y)
        {
            int zoom = ParseZoom(req);
            var peaks = await _peaksCollection.FetchByTiles([(x, y)], zoom);

            var features = peaks.Select(p => p.ToFeature()).ToList();
            var featureCollection = new FeatureCollection(features);

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }

        private static int ParseZoom(HttpRequestData req)
        {
            var success = int.TryParse(req.Query["zoom"], out int zoom);
            return success ? zoom : DefaultZoom;
        }
    }
}
