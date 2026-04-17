using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Services;
using API.Utils;

namespace API.Endpoints.AdminBoundaries;

public class GetAdminBoundariesByGrid(
    AdminBoundariesCollectionClient adminBoundariesCollectionClient,
    IConfiguration configuration)
{
    private const int DefaultZoom = 6;
    private const int DefaultAdminLevel = 2;

    [OpenApiOperation(tags: ["AdminBoundaries"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(int), Required = false)]
    [OpenApiParameter(name: "adminLevel", In = ParameterLocation.Query, Type = typeof(int), Required = false,
        Description = "OSM admin_level (e.g. 2 = country, 4 = state/region). See https://wiki.openstreetmap.org/wiki/Key:admin_level")]
    [OpenApiParameter(name: "forceRefresh", In = ParameterLocation.Query, Type = typeof(bool), Required = false,
        Description = "When true, wipes the tile cache and re-fetches from Overpass. Requires x-admin-key header.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = false)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
        Description = "A GeoJson FeatureCollection with administrative boundaries for the given OSM admin_level.")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetAdminBoundariesByGrid))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "boundaries/{x}/{y}")] HttpRequestData req, int x, int y, CancellationToken cancellationToken)
    {
        try
        {
            var zoom = ParseIntQuery(req, "zoom", DefaultZoom);
            var adminLevel = ParseIntQuery(req, "adminLevel", DefaultAdminLevel);
            var forceRefresh = string.Equals(req.Query["forceRefresh"], "true", StringComparison.OrdinalIgnoreCase);

            if (forceRefresh && !IsAuthorized(req))
                return req.CreateResponse(HttpStatusCode.Unauthorized);

            IEnumerable<Shared.Models.StoredFeature> boundaries;
            if (forceRefresh)
                boundaries = await adminBoundariesCollectionClient.RefreshTile(x, y, adminLevel, zoom, cancellationToken);
            else
                boundaries = await adminBoundariesCollectionClient.FetchByTiles([(x, y)], adminLevel, zoom, false, cancellationToken);

            var featureCollection = new FeatureCollection(boundaries
                .Where(b => !b.Id.StartsWith("empty-"))
                .Select(b => b.ToFeature()).ToList());

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(featureCollection);
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return RequestCancellation.CreateCancelledResponse(req);
        }
    }

    private bool IsAuthorized(HttpRequestData req)
    {
        var adminKey = configuration.GetValue<string>("AdminApiKey");
        if (string.IsNullOrEmpty(adminKey))
            return false;
        return req.Headers.TryGetValues("x-admin-key", out var values)
            && values.FirstOrDefault() == adminKey;
    }

    private static int ParseIntQuery(HttpRequestData req, string name, int defaultValue)
    {
        return int.TryParse(req.Query[name], out var value) ? value : defaultValue;
    }
}
