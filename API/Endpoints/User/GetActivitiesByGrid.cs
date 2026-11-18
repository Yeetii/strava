using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Geo;
using Shared.Services.Shards;
using API.Utils;

namespace API.Endpoints.User;

public class GetActivitiesByGrid(ShardFeatureClient _shardFeatureClient)
{
    // The legacy PathsCollectionClient stored and served path features at zoom 11.
    private const int PathTileZoom = 11;

    [OpenApiOperation(tags: ["Activities"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection))]
    [Function("GetActivitiesByGrid")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "activities/{x}/{y}")] HttpRequestData req,
        int x, int y,
        CancellationToken cancellationToken)
    {
        try
        {
            // Path features live in the local filesystem shards, not in PostGIS. Map the
            // requested zoom-11 tile onto the shard stack's canonical zoom before loading it.
            var shardKeys = SlippyTileCalculator.GetIntersectingTileKeys(
                PathTileZoom, x, y, _shardFeatureClient.CanonicalZoom);
            var activities = await _shardFeatureClient.GetFeaturesForShards(shardKeys, cancellationToken);
            var featureCollection = new FeatureCollection(activities);
            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return RequestCancellation.CreateCancelledResponse(req);
        }
    }
}