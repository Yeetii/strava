using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Models;
using Shared.Services;

namespace API
{
    public class GetPeaksByGrid(CollectionClient<StoredFeature> _peaksCollection)
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
            var response = req.CreateResponse();
            int zoom = ParseZoom(req);

            var (NW, SE) = GetTileBounds(x, y, zoom);
            var peaks = await _peaksCollection.FetchWithinRectangle(NW, SE);
            var featureCollection = new FeatureCollection
            {
                Features = peaks.Select(x => x.ToFeature())
            };

            response.StatusCode = HttpStatusCode.OK;
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }

        private static int ParseZoom(HttpRequestData req)
        {
            var success = int.TryParse(req.Query["zoom"], out int zoom);
            return success ? zoom : DefaultZoom;
        }

        public static (int tileX, int tileY) Wgs84ToSlippyMapTile(Coordinate coordinate, int zoom)
        {
            var lat = coordinate.Lat;
            var lon = coordinate.Lng;
            // Ensure latitude is clamped between -85.05112878 and 85.05112878 (Web Mercator limits)
            lat = Math.Max(Math.Min(lat, 85.05112878), -85.05112878);

            double latRad = lat * Math.PI / 180.0;

            // Number of tiles at the given zoom level
            int n = 1 << zoom;

            int tileX = (int)((lon + 180.0) / 360.0 * n);
            int tileY = (int)((1.0 - Math.Log(Math.Tan(latRad) + 1.0 / Math.Cos(latRad)) / Math.PI) / 2.0 * n);

            return (tileX, tileY);
        }

        // Convert tile index to WGS84 latitude and longitude
        public static Coordinate TileXYToLatLon(int tileX, int tileY, int zoom)
        {
            int n = 1 << zoom; // 2^zoom
            double lon = tileX / (double)n * 360.0 - 180.0;
            double latRad = Math.Atan(Math.Sinh(Math.PI * (1 - 2 * tileY / (double)n)));
            double lat = latRad * 180.0 / Math.PI;
            return new Coordinate(lon, lat);
        }

        // Get the NW and SE corners of a Slippy Map tile
        public static (Coordinate NW, Coordinate SE) GetTileBounds(int tileX, int tileY, int zoom)
        {
            // NW corner (top-left): tileX, tileY
            var NW = TileXYToLatLon(tileX, tileY, zoom);
            
            // SE corner (bottom-right): tileX + 1, tileY + 1
            var SE = TileXYToLatLon(tileX + 1, tileY + 1, zoom);

            return (NW, SE);
        }
    }
}
