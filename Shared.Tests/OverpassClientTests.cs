using System.Reflection;
using System.Text.Json;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging.Abstractions;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class OverpassClientTests
{
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

    private sealed class MirrorHttpMessageHandler(Dictionary<string, object[]> responsesByHost) : HttpMessageHandler
    {
        private readonly Dictionary<string, Queue<object>> _responsesByHost = responsesByHost.ToDictionary(
            pair => pair.Key,
            pair => new Queue<object>(pair.Value),
            StringComparer.OrdinalIgnoreCase);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
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
                _ => throw new InvalidOperationException($"Unsupported response type {next.GetType().FullName}.")
            });
        }
    }
}
