using Microsoft.AspNetCore.Mvc;
using Stigar.Api.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Builder;
using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Http;
using Stigar.Core.Geo;
using Stigar.Core.Services.Shards;
using Stigar.Api.Utils;

namespace Stigar.Api.Endpoints.User;

public class GetActivitiesByGrid(ShardFeatureClient _shardFeatureClient)
{
    // The legacy PathsCollectionClient stored and served path features at zoom 11.
    private const int PathTileZoom = 11;
    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/activities/{x}/{y}", (HttpContext context, [FromServices] GetActivitiesByGrid endpoint, int x, int y) => endpoint.Handle(context, x, y, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, int x, int y, CancellationToken cancellationToken)
    {
        try
        {
            // Path features live in the local filesystem shards, not in PostGIS. Map the
            // requested zoom-11 tile onto the shard stack's canonical zoom before loading it.
            var shardKeys = SlippyTileCalculator.GetIntersectingTileKeys(
                PathTileZoom, x, y, _shardFeatureClient.CanonicalZoom);
            var activities = await _shardFeatureClient.GetFeaturesForShards(shardKeys, cancellationToken);
            var featureCollection = new FeatureCollection(activities);
            return TypedResults.Ok(featureCollection);
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return TypedResults.NoContent();
        }
    }
}