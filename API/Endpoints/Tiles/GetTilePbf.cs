using System.Net;
using API.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.OpenApi.Models;
using Shared.Constants;
using Shared.Services.Shards;

namespace API.Endpoints.Tiles;

public class GetTilePbf(BlobTileService blobTileService, IConfiguration configuration)
{
    [OpenApiOperation(tags: ["Tiles"])]
    [OpenApiParameter(name: "z", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.OK, Description = "Gzipped MVT tile")]
    [Function(nameof(GetTilePbf))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "tiles/{z}/{x}/{y}.pbf")] HttpRequestData req,
        int z,
        int x,
        int y,
        CancellationToken cancellationToken)
    {
        try
        {
            if (!configuration.GetValue<bool>(AppConfig.BlobTilesEnabled))
                return req.CreateResponse(HttpStatusCode.NotFound);

            var tile = await blobTileService.BuildTileAsync(z, x, y, cancellationToken);
            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "application/x-protobuf");
            response.Headers.Add("Content-Encoding", "gzip");
            response.Headers.Add("Cache-Control", "public,max-age=60");
            await response.Body.WriteAsync(tile, cancellationToken);
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            return RequestCancellation.CreateCancelledResponse(req);
        }
    }
}
