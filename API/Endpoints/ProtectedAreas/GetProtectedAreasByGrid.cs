using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;

namespace API.Endpoints.ProtectedAreas;

public class GetProtectedAreasByGrid(ProtectedAreasCollectionClient protectedAreasCollectionClient)
{
    private const int DefaultZoom = 8;

    [OpenApiOperation(tags: ["ProtectedAreas"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(double), Required = false)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
        Description = "A GeoJson FeatureCollection with nature reserves and national parks.")]
    [Function(nameof(GetProtectedAreasByGrid))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "protectedAreas/{x}/{y}")] HttpRequestData req, int x, int y)
    {
        var zoom = ParseZoom(req);
        var protectedAreas = await protectedAreasCollectionClient.FetchByTiles([(x, y)], zoom);
        var featureCollection = new FeatureCollection(protectedAreas.Select(area => area.ToFeature()).ToList());

        var response = req.CreateResponse(HttpStatusCode.OK);
        await response.WriteAsJsonAsync(featureCollection);
        return response;
    }

    private static int ParseZoom(HttpRequestData req)
    {
        var success = int.TryParse(req.Query["zoom"], out var zoom);
        return success ? zoom : DefaultZoom;
    }
}