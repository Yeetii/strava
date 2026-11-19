using System.Text;
using Stigar.Api.Endpoints.Tiles;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.DependencyInjection;
using Stigar.Core.Constants;

namespace Stigar.Workers.Tests;

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

    [Theory]
    [InlineData("https://stigar.erikmagnusson.com")]
    [InlineData("https://peakshunters.erikmagnusson.com")]
    [InlineData("http://localhost:5173")]
    public async Task GetTrailsPmtiles_SupportsCorsPreflightAndExposedHeaders(string origin)
    {
        var root = Path.Combine(Path.GetTempPath(), "trails-cors-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var pmtilesPath = Path.Combine(root, "trails.pmtiles");
            File.WriteAllBytes(pmtilesPath, Encoding.UTF8.GetBytes("PMTiles test file"));

            var builder = WebApplication.CreateBuilder();
            builder.Configuration[AppConfig.RaceTilesOutputPath] = pmtilesPath;

            builder.Services.AddCors(options =>
            {
                options.AddPolicy(GetTrailsPmtiles.CorsPolicy, policy =>
                {
                    policy.SetIsOriginAllowed(_ => true)
                        .AllowAnyHeader()
                        .WithMethods("GET", "HEAD", "OPTIONS")
                        .AllowCredentials()
                        .WithExposedHeaders("Content-Range", "Content-Length", "ETag", "Accept-Ranges");
                });
            });

            var app = builder.Build();
            app.Use((context, next) =>
            {
                if (context.Request.Headers.ContainsKey("Access-Control-Request-Private-Network") ||
                    context.Request.Headers.ContainsKey("Origin"))
                {
                    context.Response.Headers.Append("Access-Control-Allow-Private-Network", "true");
                }

                return next(context);
            });
            app.UseCors();
            app.MapGetTrailsPmtiles(app.Configuration);
            app.Urls.Add("http://127.0.0.1:0");
            await app.StartAsync();

            try
            {
                var addresses = app.Services.GetRequiredService<Microsoft.AspNetCore.Hosting.Server.IServer>()
                    .Features.Get<Microsoft.AspNetCore.Hosting.Server.Features.IServerAddressesFeature>()!.Addresses;
                using var client = new HttpClient { BaseAddress = new Uri(addresses.Single()) };

                // 1. Preflight OPTIONS
                var preflight = new HttpRequestMessage(HttpMethod.Options, GetTrailsPmtiles.Route);
                preflight.Headers.Add("Origin", origin);
                preflight.Headers.Add("Access-Control-Request-Method", "GET");
                preflight.Headers.Add("Access-Control-Request-Headers", "range");
                preflight.Headers.Add("Access-Control-Request-Private-Network", "true");
                var preflightResponse = await client.SendAsync(preflight);

                Assert.Equal(System.Net.HttpStatusCode.NoContent, preflightResponse.StatusCode);
                Assert.Equal(origin, Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Origin")));
                Assert.Equal("true", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Credentials")));
                Assert.Equal("true", Assert.Single(preflightResponse.Headers.GetValues("Access-Control-Allow-Private-Network")));

                // 2. GET request with Range
                var get = new HttpRequestMessage(HttpMethod.Get, GetTrailsPmtiles.Route);
                get.Headers.Add("Origin", origin);
                get.Headers.Add("Range", "bytes=0-3");
                var getResponse = await client.SendAsync(get);

                Assert.Equal(System.Net.HttpStatusCode.PartialContent, getResponse.StatusCode);
                Assert.Equal(origin, Assert.Single(getResponse.Headers.GetValues("Access-Control-Allow-Origin")));
                var exposeHeaders = Assert.Single(getResponse.Headers.GetValues("Access-Control-Expose-Headers"));
                Assert.Contains("Content-Range", exposeHeaders);
                Assert.Contains("Accept-Ranges", exposeHeaders);

                // 3. HEAD request
                var head = new HttpRequestMessage(HttpMethod.Head, GetTrailsPmtiles.Route);
                head.Headers.Add("Origin", origin);
                var headResponse = await client.SendAsync(head);
                Assert.Equal(System.Net.HttpStatusCode.OK, headResponse.StatusCode);
                Assert.Equal(origin, Assert.Single(headResponse.Headers.GetValues("Access-Control-Allow-Origin")));
            }
            finally
            {
                await app.DisposeAsync();
            }
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }
}
