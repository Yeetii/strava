using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi.Models;
using Shared.Services;
using API.Utils;

namespace API.Endpoints.Paths;

public class GetPathsByGrid([FromKeyedServices(FeatureKinds.Path)] TiledCollectionClient _pathsCollectionClient)
{
    [OpenApiOperation(tags: ["Paths"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection))]
    [Function("GetPathsByGrid")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "paths/{x}/{y}")] HttpRequestData req,
        int x, int y,
        CancellationToken cancellationToken)
    {
        try
        {
            var paths = await _pathsCollectionClient.FetchByTiles([(x, y)], cancellationToken: cancellationToken);
            var featureCollection = new FeatureCollection(paths.Select(p => p.ToFeature()));
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