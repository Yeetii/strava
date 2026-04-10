using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using API.Utils;

namespace API.Endpoints.OverpassCache;

public class GetOverpassCache(OverpassCacheCollectionClient _overpassCacheCollection)
{
    private const int DefaultZoom = 11;

    [OpenApiOperation(tags: ["OverpassCache"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "Zoom level used for tiling (default: 11).")]
    [OpenApiParameter(name: "query", In = ParameterLocation.Query, Type = typeof(string), Required = true,
        Description = "Full Overpass QL query. Use {{bbox}} as a placeholder for the tile bounding box (minLat,minLon,maxLat,maxLon).")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
        Description = "A GeoJSON FeatureCollection with the results of the Overpass query for the given tile.")]
    [Function(nameof(GetOverpassCache))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "overpassCache/{x}/{y}")] HttpRequestData req,
        int x, int y,
        CancellationToken cancellationToken)
    {
        try
        {
            var query = req.Query["query"];
            if (string.IsNullOrWhiteSpace(query))
            {
                var badRequest = req.CreateResponse(HttpStatusCode.BadRequest);
                await badRequest.WriteStringAsync("The 'query' query parameter is required.", cancellationToken);
                return badRequest;
            }

            var zoom = ParseZoom(req);
            var featureCollection = await _overpassCacheCollection.FetchByTile(x, y, zoom, query, cancellationToken);

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(featureCollection, cancellationToken);
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return RequestCancellation.CreateCancelledResponse(req);
        }
    }

    private static int ParseZoom(HttpRequestData req)
    {
        var success = int.TryParse(req.Query["zoom"], out var zoom);
        return success ? zoom : DefaultZoom;
    }
}
