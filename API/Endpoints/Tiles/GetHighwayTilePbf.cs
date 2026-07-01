using System.Net;
using API.Utils;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Shared.Services.Shards;
using System.Globalization;
using System.Diagnostics;

namespace API.Endpoints.Tiles;

public class GetHighwayTilePbf(BlobTileService blobTileService, IConfiguration configuration, ILogger<GetHighwayTilePbf> logger)
{
    [OpenApiOperation(tags: ["Tiles"])]
    [OpenApiParameter(name: "z", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "x", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "y", In = ParameterLocation.Path, Type = typeof(int), Required = true)]
    [OpenApiParameter(name: "forceRefresh", In = ParameterLocation.Query, Type = typeof(bool), Required = false,
        Description = "When true, refreshes the highway tile. Canonical z12 shards are re-fetched from Overpass; lower zoom tiles rebuild from existing shard blobs only. Requires x-admin-key header.")]
    [OpenApiParameter(name: "x-admin-key", In = ParameterLocation.Header, Type = typeof(string), Required = false)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.OK, Description = "Gzipped MVT tile")]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.BadRequest)]
    [OpenApiResponseWithoutBody(statusCode: HttpStatusCode.Unauthorized)]
    [Function(nameof(GetHighwayTilePbf))]
    public async Task<HttpResponseData> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "tiles/highways/{z}/{x}/{y}.pbf")] HttpRequestData req,
        CancellationToken cancellationToken)
    {
        var start = Stopwatch.StartNew();
        var invocationId = req.FunctionContext.InvocationId;
        int z = default;
        int x = default;
        int y = default;
        var forceRefresh = false;

        try
        {
            if (!TryParseTileCoordinates(req, out z, out x, out y))
                return req.CreateResponse(HttpStatusCode.BadRequest);

            forceRefresh = string.Equals(req.Query["forceRefresh"], "true", StringComparison.OrdinalIgnoreCase);
            if (forceRefresh && !IsAuthorized(req))
                return req.CreateResponse(HttpStatusCode.Unauthorized);

            var tile = forceRefresh
                ? await blobTileService.RefreshTileAsync(z, x, y, cancellationToken)
                : await blobTileService.BuildTileAsync(z, x, y, cancellationToken);

            logger.LogInformation(
                "GetHighwayTilePbf succeeded for z{Z}/{X}/{Y} (forceRefresh={ForceRefresh}, invocationId={InvocationId}) in {ElapsedMs}ms",
                z,
                x,
                y,
                forceRefresh,
                invocationId,
                start.ElapsedMilliseconds);

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "application/x-protobuf");
            response.Headers.Add("Content-Encoding", "gzip");
            if (tile.SourceWrittenAt is not null)
            {
                response.Headers.Add("X-Tile-Source-Written-At", tile.SourceWrittenAt.Value.ToString("O"));
            }
            if (forceRefresh)
            {
                response.Headers.Add("Cache-Control", "no-store");
            }
            else if (tile.IsComplete)
            {
                response.Headers.Add("Cache-Control", "public,max-age=60");
            }
            else
            {
                response.Headers.Add("Cache-Control", "no-store");
                response.Headers.Add("X-Tile-Incomplete", "true");
            }

            await response.Body.WriteAsync(tile.Payload, cancellationToken);
            return response;
        }
        catch (Exception ex) when (RequestCancellation.IsCancellation(ex, cancellationToken))
        {
            logger.LogWarning(
                ex,
                "GetHighwayTilePbf cancelled for z{Z}/{X}/{Y} (forceRefresh={ForceRefresh}, invocationId={InvocationId}, elapsedMs={ElapsedMs}, cancellationRequested={CancellationRequested})",
                z,
                x,
                y,
                forceRefresh,
                invocationId,
                start.ElapsedMilliseconds,
                cancellationToken.IsCancellationRequested);
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

    private static bool TryParseTileCoordinates(HttpRequestData req, out int z, out int x, out int y)
    {
        z = default;
        x = default;
        y = default;

        var segments = req.Url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);
        if (segments.Length < 3)
            return false;

        var ySegment = segments[^1];
        if (!ySegment.EndsWith(".pbf", StringComparison.OrdinalIgnoreCase))
            return false;

        return int.TryParse(segments[^3], NumberStyles.None, CultureInfo.InvariantCulture, out z)
            && int.TryParse(segments[^2], NumberStyles.None, CultureInfo.InvariantCulture, out x)
            && int.TryParse(ySegment[..^4], NumberStyles.None, CultureInfo.InvariantCulture, out y);
    }
}
