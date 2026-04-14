using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using API.Utils;

namespace API.Endpoints.AdminBoundaries;

public class GetAdminBoundariesByGrid(AdminBoundariesCollectionClient adminBoundariesCollectionClient)
{
    private const int DefaultZoom = 6;
    private const int DefaultAdminLevel = 2;

    [OpenApiOperation(tags: ["AdminBoundaries"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(int), Required = false)]
    [OpenApiParameter(name: "adminLevel", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "OSM admin_level (e.g. 2 = country, 4 = state/region). See https://wiki.openstreetmap.org/wiki/Key:admin_level")]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
        Description = "A GeoJson FeatureCollection with administrative boundaries for the given OSM admin_level.")]
    [Function(nameof(GetAdminBoundariesByGrid))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "boundaries/{x}/{y}")] HttpRequestData req, int x, int y, CancellationToken cancellationToken)
    {
        try
        {
            var zoom = ParseIntQuery(req, "zoom", DefaultZoom);
            var adminLevel = ParseIntQuery(req, "adminLevel", DefaultAdminLevel);
            var boundaries = await adminBoundariesCollectionClient.FetchByTiles([(x, y)], adminLevel, zoom, cancellationToken);
            var featureCollection = new FeatureCollection(boundaries.Select(b => b.ToFeature()).ToList());

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return RequestCancellation.CreateCancelledResponse(req);
        }
    }

    private static int ParseIntQuery(HttpRequestData req, string name, int defaultValue)
    {
        return int.TryParse(req.Query[name], out var value) ? value : defaultValue;
    }
}
