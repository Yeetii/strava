using Microsoft.AspNetCore.Mvc;
using API.Http;
using System.Net;
using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services.Postgres.Spatial;
using API.Utils;

namespace API
{
    public class GetPeaks(IPeakRepository _peaks)
    {
        const int MinRadiusMetres = (int)40E3;
        const int MaxRadiusMetres = (int)100E3;
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/peaks", (HttpContext context, [FromServices] GetPeaks endpoint) => endpoint.Handle(context, context.RequestAborted));

        public async Task<IResult> Handle(HttpContext context, CancellationToken cancellationToken)
        {
            try
            {
                if (!ParseCenter(context, out Coordinate center))
                    return TypedResults.BadRequest("Invalid lat and lon, must be valid decimal degree format");
                int radius = ParseRadius(context);

                var peaks = await _peaks.FindWithinRadius(center, radius, cancellationToken);
                var features = peaks.Select(x => x.ToFeature()).ToList();
                var featureCollection = new FeatureCollection(features);

                return TypedResults.Ok(featureCollection);
            }
            catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
            {
                return TypedResults.NoContent();
            }
        }

        private static int ParseRadius(HttpContext req)
        {
            _ = int.TryParse(req.Request.Query["radius"], out int radius);
            radius = Math.Min(radius, MaxRadiusMetres);
            radius = Math.Max(radius, MinRadiusMetres);
            return radius;
        }

        private static bool ParseCenter(HttpContext req, out Coordinate center)
        {
            var latSuccess = double.TryParse(req.Request.Query["lat"], out double lat);
            var lonSuccess = double.TryParse(req.Request.Query["lon"], out double lon);
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
