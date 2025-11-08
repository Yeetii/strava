using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.Paths;

public class GetPathsByGrid(PathsCollectionClient _pathsCollectionClient)
{
    [OpenApiOperation(tags: ["Paths"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection))]
    [Function("GetPathsByGrid")]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "paths/{x}/{y}")] HttpRequestData req,
        int x, int y)
    {
        var paths = await _pathsCollectionClient.FetchByTiles([(x, y)]);
        var features = paths.Features.Select(f => f).ToList();
        var featureCollection = new FeatureCollection(features);
        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(featureCollection);
        return response;
    }
}