using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API
{
    public class GetPeaks(PeaksCollectionClient _peaksCollection)
    {
        const int MinRadiusMetres = (int)40E3;
        const int MaxRadiusMetres = (int)100E3;

        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "lat", In = ParameterLocation.Query, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "lon", In = ParameterLocation.Query, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "radius", In = ParameterLocation.Query, Type = typeof(double), Required = false)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
            Description = "A GeoJson FeatureCollection with peaks. If there is a valid session cookie, the peaks are populated with the summited property.")]
        [Function("GetPeaks")]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "peaks")] HttpRequestData req)
        {
            // Probably won't need this in production if I move the API to the same domain as the frontend
            string? sessionId = req.Cookies.FirstOrDefault(cookie => cookie.Name == "session")?.Value;
            if (!ParseCenter(req, out Coordinate center))
            {
                var badResponse = req.CreateResponse(HttpStatusCode.BadRequest);
                await badResponse.WriteStringAsync("Invalid lat and lon, must be valid decimal degree format");
                return badResponse;
            }
            int radius = ParseRadius(req);

            var peaks = await _peaksCollection.GeoSpatialFetch(center, radius);
            var features = peaks.Select(x => x.ToFeature()).ToList();
            var featureCollection = new FeatureCollection(features);

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Access-Control-Allow-Credentials", "true");
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }

        private static int ParseRadius(HttpRequestData req)
        {
            _ = int.TryParse(req.Query["radius"], out int radius);
            radius = Math.Min(radius, MaxRadiusMetres);
            radius = Math.Max(radius, MinRadiusMetres);
            return radius;
        }

        private static bool ParseCenter(HttpRequestData req, out Coordinate center)
        {
            var latSuccess = double.TryParse(req.Query["lat"], out double lat);
            var lonSuccess = double.TryParse(req.Query["lon"], out double lon);
            if (!latSuccess || !lonSuccess || Math.Abs(lat) > 90 || Math.Abs(lon) > 180)
            {
                center = new Coordinate(0, 0);
                return false;
            }
            center = new Coordinate(lon, lat);
            return true;
        }
    }
}
