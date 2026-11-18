using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Geo;
using Shared.Services.Postgres.Spatial;
using API.Utils;

namespace API.Endpoints.Peaks
{
    public class GetPeaksByGrid(IPeakRepository _peaks)
    {
        // Peaks are stored at zoom 11; the optional zoom selects a view over those tiles.
        const int DefaultZoom = 11;

        [OpenApiOperation(tags: ["Peaks"])]
        [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
        [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(double), Required = false)]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
            Description = "A GeoJson FeatureCollection with peaks.")]
        [Function(nameof(GetPeaksByGrid))]
        public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "peaks/{x}/{y}")] HttpRequestData req, int x, int y, CancellationToken cancellationToken)
        {
            try
            {
                int zoom = ParseZoom(req);
                IEnumerable<Feature> features;
                if (zoom == DefaultZoom)
                {
                    var peaks = await _peaks.GetByTile(x, y, cancellationToken);
                    features = peaks.Select(p => p.ToFeature()).ToList();
                }
                else
                {
                    // Map a non-canonical tile onto the zoom-11 storage tiles, then filter
                    // on the centroid exactly as the legacy FetchByTiles path did.
                    var storageKeys = SlippyTileCalculator.GetIntersectingTileKeys(zoom, x, y, DefaultZoom);
                    var matched = new List<Feature>();
                    foreach (var (storageX, storageY) in storageKeys)
                    {
                        var peaks = await _peaks.GetByTile(storageX, storageY, cancellationToken);
                        matched.AddRange(peaks
                            .Where(p => SlippyTileCalculator.WGS84ToTileIndex(p.ResolvedCentroid, zoom) == (x, y))
                            .Select(p => p.ToFeature()));
                    }
                    features = matched;
                }

                var featureCollection = new FeatureCollection(features);

                var response = req.CreateResponse(HttpStatusCode.OK);
                await response.WriteAsJsonAsync(featureCollection);
                return response;
            }
            catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
            {
                return RequestCancellation.CreateCancelledResponse(req);
            }
        }

        private static int ParseZoom(HttpRequestData req)
        {
            var success = int.TryParse(req.Query["zoom"], out int zoom);
            return success ? zoom : DefaultZoom;
        }
    }
}
