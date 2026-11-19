using Microsoft.AspNetCore.Mvc;
using API.Http;
using System.Net;
using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Services.Postgres.Spatial;
using API.Utils;

namespace API.Endpoints.Peaks
{
    public class GetPeaksByGrid(IPeakRepository _peaks)
    {
        // Peaks are stored at zoom 11; the optional zoom selects a view over those tiles.
        const int DefaultZoom = 11;
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/peaks/{x}/{y}", (HttpContext context, [FromServices] GetPeaksByGrid endpoint, int x, int y) => endpoint.Handle(context, x, y, context.RequestAborted));

        public async Task<IResult> Handle(HttpContext context, int x, int y, CancellationToken cancellationToken)
        {
            try
            {
                int zoom = ParseZoom(context);
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

                return TypedResults.Ok(featureCollection);
            }
            catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
            {
                return TypedResults.NoContent();
            }
        }

        private static int ParseZoom(HttpContext req)
        {
            var success = int.TryParse(req.Request.Query["zoom"], out int zoom);
            return success ? zoom : DefaultZoom;
        }
    }
}
