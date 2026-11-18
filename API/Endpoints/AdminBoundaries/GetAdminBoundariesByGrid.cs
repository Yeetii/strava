using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using API.Utils;
using Shared.Geo;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres.Admin;
using Shared.Services.Postgres.Spatial;
using Shared.Services.Postgres.Spatial.Ingestion;

namespace API.Endpoints.AdminBoundaries;

public class GetAdminBoundariesByGrid(
    IAdminBoundaryRepository adminBoundaries,
    IOsmFeatureIngestionService ingestionService,
    IOsmTileQueryRepository tileQueries,
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
        Description = "When true, clears the OSM tile-query markers covering this tile and re-fetches from Overpass. Requires x-admin-key header.")]
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

            if (forceRefresh)
                await RefreshTileAsync(x, y, zoom, adminLevel, cancellationToken);

            var boundaries = await adminBoundaries.GetByTile(x, y, adminLevel, zoom, cancellationToken);

            var featureCollection = new FeatureCollection(boundaries
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

    private async Task RefreshTileAsync(int x, int y, int zoom, int adminLevel, CancellationToken cancellationToken)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);
        var storageZoom = AdminOsmConstants.AdminBoundaryStorageZoom;

        var (swX, swY) = SlippyTileCalculator.WGS84ToTileIndex(southWest, storageZoom);
        var (neX, neY) = SlippyTileCalculator.WGS84ToTileIndex(northEast, storageZoom);

        for (var tileX = Math.Min(swX, neX); tileX <= Math.Max(swX, neX); tileX++)
        {
            for (var tileY = Math.Min(swY, neY); tileY <= Math.Max(swY, neY); tileY++)
            {
                await tileQueries.Delete(
                    FeatureKinds.AdminBoundary,
                    storageZoom,
                    tileX,
                    tileY,
                    cancellationToken);
            }
        }

        var center = new Coordinate(
            (southWest.Lng + northEast.Lng) / 2,
            (southWest.Lat + northEast.Lat) / 2);
        await ingestionService.EnsureAdminBoundariesAsync(
            [southWest, northEast, center],
            adminLevel,
            cancellationToken);
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
