using Microsoft.AspNetCore.Mvc;
using API.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using System.Net;
using BAMCIS.GeoJSON;
using Shared.Geo;
using Shared.Services.Postgres.Spatial;
using API.Utils;

namespace API.Endpoints.ProtectedAreas;

public class GetProtectedAreasByGrid(IProtectedAreaRepository protectedAreas)
{
    // Protected areas are stored at zoom 8; the optional zoom selects a view over those tiles.
    private const int DefaultZoom = 8;
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/protectedAreas/{x}/{y}", (HttpContext context, [FromServices] GetProtectedAreasByGrid endpoint, int x, int y) => endpoint.Handle(context, x, y, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, int x, int y, CancellationToken cancellationToken)
    {
        try
        {
            var zoom = ParseZoom(context);
            IEnumerable<Feature> features;
            if (zoom == DefaultZoom)
            {
                var areas = await protectedAreas.GetByTile(x, y, cancellationToken);
                features = areas.Select(area => area.ToFeature()).ToList();
            }
            else
            {
                var storageKeys = SlippyTileCalculator.GetIntersectingTileKeys(zoom, x, y, DefaultZoom);
                var matched = new List<Feature>();
                foreach (var (storageX, storageY) in storageKeys)
                {
                    var areas = await protectedAreas.GetByTile(storageX, storageY, cancellationToken);
                    matched.AddRange(areas
                        .Where(area => SlippyTileCalculator.WGS84ToTileIndex(area.ResolvedCentroid, zoom) == (x, y))
                        .Select(area => area.ToFeature()));
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
        var success = int.TryParse(req.Request.Query["zoom"], out var zoom);
        return success ? zoom : DefaultZoom;
    }
}