using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Geo;
using Shared.Services.Postgres.Spatial;
using API.Utils;

namespace API.Endpoints.ProtectedAreas;

public class GetProtectedAreasByGrid(IProtectedAreaRepository protectedAreas)
{
    // Protected areas are stored at zoom 8; the optional zoom selects a view over those tiles.
    private const int DefaultZoom = 8;

    [OpenApiOperation(tags: ["ProtectedAreas"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(double), Required = true)]
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(double), Required = false)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
        Description = "A GeoJson FeatureCollection with nature reserves and national parks.")]
    [Function(nameof(GetProtectedAreasByGrid))]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "protectedAreas/{x}/{y}")] HttpRequestData req, int x, int y, CancellationToken cancellationToken)
    {
        try
        {
            var zoom = ParseZoom(req);
            IEnumerable<Feature> features;
            if (zoom == DefaultZoom)
            {
                var areas = await protectedAreas.GetByTile(x, y, cancellationToken);
                features = areas.Select(area => area.ToFeature()).ToList();
            }
            else
            {
                var storageKeys = SlippyTileCalculator.GetIntersectingTileKeys(zoom, x, y, DefaultZoom);
                var matched = new List<Feature>();
                foreach (var (storageX, storageY) in storageKeys)
                {
                    var areas = await protectedAreas.GetByTile(storageX, storageY, cancellationToken);
                    matched.AddRange(areas
                        .Where(area => SlippyTileCalculator.WGS84ToTileIndex(area.ResolvedCentroid, zoom) == (x, y))
                        .Select(area => area.ToFeature()));
                }
                features = matched;
            }
            var featureCollection = new FeatureCollection(features);

            var response = req.CreateResponse(HttpStatusCode.OK);
            await response.WriteAsJsonAsync(featureCollection);
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