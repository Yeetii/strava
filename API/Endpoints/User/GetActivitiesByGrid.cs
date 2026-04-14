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

namespace API.Endpoints.User;

public class GetActivitiesByGrid([FromKeyedServices(FeatureKinds.Path)] TiledCollectionClient _pathsCollectionClient)
{
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
            var activities = await _pathsCollectionClient.FetchByTiles([(x, y)], cancellationToken: cancellationToken);
            var featureCollection = new FeatureCollection(activities.Select(a => a.ToFeature()));
            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Access-Control-Allow-Credentials", "true");
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return RequestCancellation.CreateCancelledResponse(req);
        }
    }
}