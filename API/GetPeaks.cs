using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API
{
    public class GetPeaks(CollectionClient<StoredFeature> _collectionClient)
    {
        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "lat", In = ParameterLocation.Query, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "lon", In = ParameterLocation.Query, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "radius", In = ParameterLocation.Query, Type = typeof(double))]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection), 
            Description = "A GeoJson FeatureCollection with peaks")]
        [Function("GetPeaks")]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "peaks")] HttpRequestData req)
        {
            var latSuccess = double.TryParse(req.Query["lat"], out double lat);
            var lonSuccess = double.TryParse(req.Query["lon"], out double lon);
            if (!latSuccess || !lonSuccess || Math.Abs(lat) > 90  || Math.Abs(lon) > 180)
                throw new ArgumentException("Invalid lat and lon, must be valid decimal degree format");
            var center = new Coordinate(lon, lat);

            const int defaultRadius = 40000;
            if (!int.TryParse(req.Query["radius"], out int radius))
                radius = defaultRadius;
            if (radius > 100E3)
                radius = (int) 100E3;
            
            var peaks = await _collectionClient.GeoSpatialFetch(center, radius);

            var featureCollection = new FeatureCollection{
                Features = peaks.Select(x => x.ToFeature())
            };

            var response = req.CreateResponse(HttpStatusCode.OK);
            // var json = JsonSerializer.Serialize(featureCollection);
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }
    }
}
