using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class RaceAssemblerTests
{
    [Fact]
    public async Task AssembleRacesAsync_IgnoresLoppkartanLocationAndCoordinates_WhenOtherDiscoveriesExist()
    {
        var geocoder = new FakeLocationGeocodingService(("Good Town", "SE"), (59.3, 18.1));
        var doc = new RaceOrganizerDocument
        {
            Id = "sample.org",
            Url = "https://sample.org",
            Discovery = new Dictionary<string, List<SourceDiscovery>>(StringComparer.OrdinalIgnoreCase)
            {
                ["runagain"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Sample Trail",
                        Distance = "10 km",
                        Country = "SE",
                        Location = "Good Town"
                    }
                ],
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Sample Trail",
                        Distance = "10 km",
                        Country = "SE",
                        Location = "Bad Town",
                        Latitude = 1.23,
                        Longitude = 4.56
                    }
                ]
            }
        };

        var races = await RaceAssembler.AssembleRacesAsync(doc, geocoder, CancellationToken.None);

        var race = Assert.Single(races);
        var point = Assert.IsType<Point>(race.Geometry);
        Assert.Equal(18.1, point.Coordinates.Longitude);
        Assert.Equal(59.3, point.Coordinates.Latitude);
        Assert.Equal("Good Town", Assert.IsType<string>(race.Properties[RaceAssembler.PropLocation]));
        Assert.Equal([("Good Town", "SE")], geocoder.Requests);
    }

    [Fact]
    public async Task AssembleRacesAsync_DropsLoppkartanOnlyFallbackCoordinates_WhenAnotherDiscoveryExists()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "sample.org",
            Url = "https://sample.org",
            Discovery = new Dictionary<string, List<SourceDiscovery>>(StringComparer.OrdinalIgnoreCase)
            {
                ["runagain"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Sample Trail",
                        Distance = "10 km",
                        Country = "SE"
                    }
                ],
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Sample Trail",
                        Distance = "10 km",
                        Country = "SE",
                        Location = "Bad Town",
                        Latitude = 1.23,
                        Longitude = 4.56
                    }
                ]
            }
        };

        var races = await RaceAssembler.AssembleRacesAsync(doc, geocodingService: null, CancellationToken.None);

        Assert.Empty(races);
    }

    [Fact]
    public async Task AssembleRacesAsync_MergesRoughlyMatchingDiscoveryDistances_ToCanonicalNormalizedValues()
    {
        var distances = new[]
        {
            "0.0 km", "0.8 km", "0.8 km", "2.0 km", "3.0 km", "5.0 km", "5.3 km", "6.0 km",
            "8.0 km", "9.0 km", "10.0 km", "10.6 km", "15.0 km", "15.9 km", "21.0 km", "21.2 km"
        };

        var doc = new RaceOrganizerDocument
        {
            Id = "sample.org",
            Url = "https://sample.org",
            Discovery = new Dictionary<string, List<SourceDiscovery>>(StringComparer.OrdinalIgnoreCase)
            {
                ["runagain"] =
                [
                    .. distances.Select(distance => new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Sample Trail",
                        Distance = distance,
                        Country = "SE",
                        Latitude = 59.3,
                        Longitude = 18.1
                    })
                ]
            }
        };

        var races = await RaceAssembler.AssembleRacesAsync(doc, geocodingService: null, CancellationToken.None);

        var race = Assert.Single(races);
        Assert.Equal("0.8 km, 2 km, 3 km, 5 km, 6 km, 8 km, 9 km, 10 km, 15 km, 21 km",
            Assert.IsType<string>(race.Properties[RaceAssembler.PropDistance]));
    }

    private sealed class FakeLocationGeocodingService((string location, string? country) match, (double lat, double lng) coords)
        : ILocationGeocodingService
    {
        private readonly (string location, string? country) _match = match;
        private readonly (double lat, double lng) _coords = coords;

        public List<(string location, string? country)> Requests { get; } = [];

        public Task<(double lat, double lng)?> GeocodeAsync(string location, string? country, CancellationToken cancellationToken)
        {
            Requests.Add((location, country));
            if (string.Equals(location, _match.location, StringComparison.Ordinal)
                && string.Equals(country, _match.country, StringComparison.Ordinal))
            {
                return Task.FromResult<(double lat, double lng)?>(_coords);
            }

            return Task.FromResult<(double lat, double lng)?>(null);
        }
    }
}