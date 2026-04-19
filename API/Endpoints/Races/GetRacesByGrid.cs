using System.Net;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.OpenApi.Models;
using Shared.Services;
using API.Utils;

namespace API.Endpoints.Races;

public class GetRacesByGrid(RaceCollectionClient racesCollectionClient)
{
    [OpenApiOperation(tags: ["Races"])]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "zoom", In = ParameterLocation.Query, Type = typeof(int), Required = false)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(FeatureCollection),
        Description = "A GeoJson FeatureCollection with trail running race routes.")]
    [Function(nameof(GetRacesByGrid))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "races/{x}/{y}")] HttpRequestData req,
        int x,
        int y,
        CancellationToken cancellationToken)
    {
        try
        {
            var zoom = ParseZoom(req);
            var races = await racesCollectionClient.FetchByTiles([(x, y)], zoom, false, cancellationToken);
            var featureCollection = new FeatureCollection(races.Select(r => r.ToFeature()).ToList());
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
        return int.TryParse(req.Query["zoom"], out var zoom)
            ? zoom
            : RaceCollectionClient.DefaultZoom;
    }
}
