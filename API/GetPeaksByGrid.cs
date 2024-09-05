using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Helpers;
using Shared.Models;
using Shared.Services;

namespace API
{
    public class GetPeaksByGrid(CollectionClient<StoredFeature> _peaksCollection, OverpassClient _overpassClient)
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

            var peaks = await _peaksCollection.QueryCollectionByXYIndex(x, y);

            if (!peaks.Any())
            {
                var (nw, se) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);

                var rawPeaks = await _overpassClient.GetPeaks(nw, se);
                peaks = RawPeakToStoredFeature(x, y, rawPeaks);
                // If upsert takes to long time, place upsert job on queue instead of blocking
                await _peaksCollection.BulkUpsert(peaks);
            }

            var featureCollection = new FeatureCollection
            {
                Features = peaks.Select(x => x.ToFeature())
            };

            response.StatusCode = HttpStatusCode.OK;
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }

        private static IEnumerable<StoredFeature> RawPeakToStoredFeature(int x, int y, IEnumerable<RawPeaks> rawPeaks)
        {
            foreach (var p in rawPeaks)
            {
                var propertiesDirty = new Dictionary<string, object?>(){
                    {"elevation", p.Tags.Elevation},
                    {"name", p.Tags.Name},
                    {"nameSapmi", p.Tags.NameSapmi},
                    {"nameAlt", p.Tags.NameAlt},
                    {"groups", new Dictionary<string, bool>()}
                };

                var properties = propertiesDirty.Where(x => x.Value != null).ToDictionary();

                yield return new StoredFeature
                {
                    Id = p.Id.ToString(),
                    X = x,
                    Y = y,
                    Properties = properties,
                    Geometry = new Geometry { Coordinates = [p.Lon, p.Lat], Type = GeometryType.Point }
                };
            };
        }

        private static int ParseZoom(HttpRequestData req)
        {
            var success = int.TryParse(req.Query["zoom"], out int zoom);
            return success ? zoom : DefaultZoom;
        }
    }
}
