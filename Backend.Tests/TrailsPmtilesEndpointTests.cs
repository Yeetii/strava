using System.Text;
using API.Endpoints.Tiles;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.DependencyInjection;

namespace Backend.Tests;

public class TrailsPmtilesEndpointTests
{
    private static DefaultHttpContext CreateContext()
    {
        var services = new ServiceCollection().AddLogging().BuildServiceProvider();
        return new DefaultHttpContext
        {
            RequestServices = services,
            Response = { Body = new MemoryStream() },
        };
    }

    [Fact]
    public async Task BuildResult_EnablesRangeProcessingAndServesExactBytes()
    {
        var root = Path.Combine(Path.GetTempPath(), "trails-endpoint-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var path = Path.Combine(root, "trails.pmtiles");
            var bytes = Encoding.UTF8.GetBytes("PMTiles").Concat(new byte[1024]).ToArray();
            bytes[10] = 42;
            File.WriteAllBytes(path, bytes);

            var context = CreateContext();
            var result = GetTrailsPmtiles.BuildResult(context, path);

            // enableRangeProcessing is the contract that makes the framework
            // answer Range requests with 206 once running under Kestrel.
            var fileResult = Assert.IsType<PhysicalFileHttpResult>(result);
            Assert.True(fileResult.EnableRangeProcessing);
            Assert.Equal("application/octet-stream", fileResult.ContentType);

            await result.ExecuteAsync(context);

            Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
            Assert.Equal("no-cache", context.Response.Headers.CacheControl.ToString());
            Assert.Equal(bytes, ((MemoryStream)context.Response.Body).ToArray());
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task BuildResult_ReturnsNotFound_WhenTileFileIsMissing()
    {
        var context = CreateContext();

        var result = GetTrailsPmtiles.BuildResult(
            context,
            Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".pmtiles"));
        await result.ExecuteAsync(context);

        Assert.Equal(StatusCodes.Status404NotFound, context.Response.StatusCode);
    }
}
