using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Shared.Constants;

namespace API.Endpoints.Tiles;

/// <summary>
/// Public, range-enabled serving of the locally built race PMTiles file.
/// The file lives on the shared race-data volume; the API only reads it.
/// CORS is applied by the host's default policy and the blob behaviour of
/// "no-cache" is preserved so clients always revalidate the tile index.
/// </summary>
public static class GetTrailsPmtiles
{
    public const string Route = "/tiles/trails.pmtiles";
    public const string ContentType = "application/octet-stream";

    public static IEndpointRouteBuilder MapGetTrailsPmtiles(
        this IEndpointRouteBuilder endpoints,
        IConfiguration configuration)
    {
        var outputPath = configuration[AppConfig.RaceTilesOutputPath]
            ?? AppConfig.DefaultRaceTilesOutputPath;

        endpoints.MapGet(Route, (HttpContext context) => BuildResult(context, outputPath))
        .AllowAnonymous()
        .WithName(nameof(GetTrailsPmtiles));

        return endpoints;
    }

    internal static IResult BuildResult(HttpContext context, string outputPath)
    {
        if (!File.Exists(outputPath))
        {
            context.Response.Headers.CacheControl = "no-store";
            return Results.NotFound();
        }

        context.Response.Headers.CacheControl = "no-cache";
        return Results.File(
            outputPath,
            contentType: ContentType,
            enableRangeProcessing: true);
    }
}
