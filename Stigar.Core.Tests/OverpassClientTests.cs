using System.Reflection;
using System.Text.Json;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging.Abstractions;
using Stigar.Core.Models;
using Stigar.Core.Services;

namespace Stigar.Core.Tests;

public class OverpassClientTests
{
    public OverpassClientTests()
    {
        var field = typeof(OverpassClient).GetField("MirrorHealthByUrl", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        ((System.Collections.IDictionary)field!.GetValue(null)!).Clear();
    }

    [Fact]
    public async Task GetHighways_IgnoresSingleEmptyMirrorWhenLaterMirrorHasData()
    {
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["overpass.openstreetmap.fr"] = [CreateOverpassJson([])],
            ["overpass-api.de"] =
            [
                CreateOverpassJson([
                    new RawPath
                    {
                        Id = 123,
                        Geometry =
                        [
                            new PathNode { Lat = 59.0, Lon = 18.0 },
                            new PathNode { Lat = 59.1, Lon = 18.1 }
                        ]
                    }
                ])
            ]
        }));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);

        var features = (await client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2))).ToList();

        var feature = Assert.Single(features);
        Assert.Equal("123", feature.Id.Value?.ToString());
    }

    [Fact]
    public async Task GetHighways_QueriesSnowmobileYesWaysAlongsideHighways()
    {
        string? requestQuery = null;
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["overpass.openstreetmap.fr"] =
            [
                CreateOverpassJson([
                    new RawPath
                    {
                        Id = 456,
                        Geometry =
                        [
                            new PathNode { Lat = 59.0, Lon = 18.0 },
                            new PathNode { Lat = 59.1, Lon = 18.1 }
                        ]
                    }
                ])
            ],
            ["overpass-api.de"] = [CreateOverpassJson([])]
        }, onRequest: uri => requestQuery = Uri.UnescapeDataString(uri.Query)));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);

        var features = (await client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2))).ToList();

        Assert.Single(features);
        Assert.NotNull(requestQuery);
        Assert.Contains("way[highway]", requestQuery, StringComparison.Ordinal);
        Assert.Contains(
            "way[\"snowmobile\"~\"^(yes|designated|permissive|official)$\"]",
            requestQuery,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task GetHighways_ThrowsWhenEmptyResultIsNotCorroborated()
    {
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["overpass.openstreetmap.fr"] = [CreateOverpassJson([])],
            ["overpass-api.de"] = [System.Net.HttpStatusCode.BadRequest]
        }));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);

        var exception = await Assert.ThrowsAsync<HttpRequestException>(() =>
            client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2)));

        Assert.Contains("unconfirmed empty data", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GetHighways_UsesNextGisMirrorWhenOthersReject()
    {
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["overpass.openstreetmap.fr"] = [System.Net.HttpStatusCode.Forbidden],
            ["overpass-api.de"] = [System.Net.HttpStatusCode.Forbidden],
            ["overpass.nextgis.com"] = [CreateOverpassJson([CreatePath()])]
        }));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);

        var features = (await client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2))).ToList();

        Assert.Single(features);
    }

    [Fact]
    public async Task GetHighways_SkipsMirrorAfterRepeatedHttpFailures()
    {
        const string blockedMirror = "https://blocked.example/api/interpreter";
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["blocked.example"] = Enumerable.Repeat<object>(System.Net.HttpStatusCode.Forbidden, 10).ToArray()
        }));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);
        SetMirrors(client, blockedMirror);

        for (var i = 0; i < 3; i++)
            await Assert.ThrowsAsync<HttpRequestException>(() =>
                client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2)));

        SetMirrors(client, blockedMirror, "https://healthy.example/api/interpreter");

        Assert.DoesNotContain(blockedMirror, GetMirrorOrder(client));
    }

    [Fact]
    public async Task GetPeaks_SkipsMirrorAfterRepeatedHttpFailures()
    {
        const string blockedMirror = "https://blocked-peaks.example/api/interpreter";
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["blocked-peaks.example"] = Enumerable.Repeat<object>(System.Net.HttpStatusCode.Forbidden, 10).ToArray()
        }));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);
        SetMirrors(client, blockedMirror);

        for (var i = 0; i < 3; i++)
            await Assert.ThrowsAsync<HttpRequestException>(() =>
                client.GetPeaks(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2)));

        SetMirrors(client, blockedMirror, "https://healthy-peaks.example/api/interpreter");

        Assert.DoesNotContain(blockedMirror, GetMirrorOrder(client));
    }

    [Fact]
    public async Task GetHighways_SkipsMirrorAfterRepeatedConnectionFailures()
    {
        var failure = new HttpRequestException("Network is unreachable", new System.Net.Sockets.SocketException(101));
        var deadRequests = 0;
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["dead.example"] = Enumerable.Repeat<object>(failure, 10).ToArray(),
            ["healthy.example"] = [CreateOverpassJson([CreatePath()])]
        }, onRequest: uri =>
        {
            if (uri.Host == "dead.example")
                deadRequests++;
        }));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);
        SetMirrors(client, "https://dead.example/api/interpreter");

        // Two failures in the first call, the third opens the circuit, then a probe of all
        // mirrors when every one is cooling down.
        for (var i = 0; i < 3; i++)
            await Assert.ThrowsAsync<HttpRequestException>(() =>
                client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2)));

        Assert.Equal(5, deadRequests);

        SetMirrors(client, "https://dead.example/api/interpreter", "https://healthy.example/api/interpreter");

        var features = (await client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2))).ToList();

        Assert.Single(features);
        Assert.Equal(5, deadRequests);
    }

    [Fact]
    public async Task GetHighways_RetriesMirrorAfterRequestTimeout()
    {
        var timeout = new TaskCanceledException("HttpClient timeout", new TimeoutException());
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["overpass.openstreetmap.fr"] = [timeout, CreateOverpassJson([CreatePath()])],
            ["overpass-api.de"] = [timeout, CreateOverpassJson([CreatePath()])]
        }));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);

        var features = (await client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2))).ToList();

        Assert.Single(features);
    }

    [Fact]
    public async Task GetHighways_PropagatesCallerCancellationFromRequest()
    {
        using var cts = new CancellationTokenSource();
        using var httpClient = new HttpClient(new MirrorHttpMessageHandler(new Dictionary<string, object[]>
        {
            ["overpass.openstreetmap.fr"] = [new OperationCanceledException(cts.Token)],
            ["overpass-api.de"] = [new OperationCanceledException(cts.Token)]
        }, onRequest: _ => cts.Cancel()));
        var client = new OverpassClient(httpClient, NullLogger<OverpassClient>.Instance);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.GetHighways(new Coordinate(18.0, 59.0), new Coordinate(18.2, 59.2), cts.Token));
    }

    [Fact]
    public void BuildGeometryFromRelation_IgnoresNullNodesInMemberGeometry()
    {
        var members = new[]
        {
            new RawProtectedAreaMember
            {
                Role = "outer",
                Geometry =
                [
                    new PathNode { Lat = 0, Lon = 0 },
                    null!,
                    new PathNode { Lat = 0, Lon = 1 },
                    new PathNode { Lat = 1, Lon = 1 }
                ]
            },
            new RawProtectedAreaMember
            {
                Role = "outer",
                Geometry =
                [
                    new PathNode { Lat = 1, Lon = 1 },
                    new PathNode { Lat = 1, Lon = 0 },
                    new PathNode { Lat = 0, Lon = 0 }
                ]
            }
        };

        var geometry = InvokeBuildGeometryFromRelation(members);

        var polygon = Assert.IsType<Polygon>(geometry);
        Assert.Equal(5, polygon.Coordinates.Single().Coordinates.Count());
    }

    private static Geometry? InvokeBuildGeometryFromRelation(IEnumerable<RawProtectedAreaMember> members)
    {
        var method = typeof(OverpassClient).GetMethod(
            "BuildGeometryFromRelation",
            BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(method);
        return (Geometry?)method!.Invoke(null, [members]);
    }

    private static void SetMirrors(OverpassClient client, params string[] urls)
    {
        var field = typeof(OverpassClient).GetField("mirrors", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(field);
        field!.SetValue(client, urls);
    }

    private static string[] GetMirrorOrder(OverpassClient client)
    {
        var method = typeof(OverpassClient).GetMethod("GetMirrorsInAttemptOrder", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(method);
        return (string[])method!.Invoke(client, null)!;
    }

    private static RawPath CreatePath() => new()
    {
        Id = 123,
        Geometry =
        [
            new PathNode { Lat = 59.0, Lon = 18.0 },
            new PathNode { Lat = 59.1, Lon = 18.1 }
        ]
    };

    private static string CreateOverpassJson(IEnumerable<RawPath> elements)
    {
        return JsonSerializer.Serialize(new
        {
            elements = elements.Select(element => new
            {
                id = element.Id,
                geometry = element.Geometry?.Select(node => new { lat = node.Lat, lon = node.Lon }) ?? [],
                tags = new Dictionary<string, string>()
            })
        });
    }

    private sealed class MirrorHttpMessageHandler(
        Dictionary<string, object[]> responsesByHost,
        Action<Uri>? onRequest = null) : HttpMessageHandler
    {
        private readonly Dictionary<string, Queue<object>> _responsesByHost = responsesByHost.ToDictionary(
            pair => pair.Key,
            pair => new Queue<object>(pair.Value),
            StringComparer.OrdinalIgnoreCase);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri is { } requestUri)
                onRequest?.Invoke(requestUri);

            var host = request.RequestUri?.Host ?? throw new InvalidOperationException("Request URI host was missing.");
            if (!_responsesByHost.TryGetValue(host, out var responses) || responses.Count == 0)
                throw new InvalidOperationException($"No more responses configured for host '{host}'.");

            var next = responses.Dequeue();
            return Task.FromResult(next switch
            {
                string content => new HttpResponseMessage(System.Net.HttpStatusCode.OK)
                {
                    Content = new StringContent(content)
                },
                System.Net.HttpStatusCode statusCode => new HttpResponseMessage(statusCode)
                {
                    Content = new StringContent(string.Empty)
                },
                Exception exception => throw exception,
                _ => throw new InvalidOperationException($"Unsupported response type {next.GetType().FullName}.")
            });
        }
    }
}
