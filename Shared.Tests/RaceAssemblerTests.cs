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

    [Fact]
    public async Task AssembleRacesAsync_PrefersMittloppDiscoveryWebsiteImageAndPrice()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "gfidrottloparforening.se",
            Url = "https://gfidrottloparforening.se",
            Discovery = new Dictionary<string, List<SourceDiscovery>>(StringComparer.OrdinalIgnoreCase)
            {
                ["mittlopp"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Landskrona halvmarathon",
                        Distance = "21.1 km",
                        Country = "SE",
                        Location = "Borstahusen, Landskrona",
                        RaceType = "running",
                        ImageUrl = "https://mittlopp.se/Upload/SubCompHeader/sh_6066.jpg?min=57",
                        StartFee = "295 kr",
                        Currency = "SEK",
                        SourceUrls =
                        [
                            "https://mittlopp.se/anm/Landskrona-halvmarathon-2026/halvmarathon?lang=sv",
                            "http://www.gfidrottloparforening.se/"
                        ],
                        Latitude = 55.9,
                        Longitude = 12.8
                    }
                ]
            }
        };

        var races = await RaceAssembler.AssembleRacesAsync(doc, geocodingService: null, CancellationToken.None);

        var race = Assert.Single(races);
        Assert.Equal("http://www.gfidrottloparforening.se/", Assert.IsType<string>(race.Properties[RaceAssembler.PropWebsite]));
        Assert.Equal("https://mittlopp.se/Upload/SubCompHeader/sh_6066.jpg?min=57", Assert.IsType<string>(race.Properties[RaceAssembler.PropImage]));
        Assert.Equal("295 kr", Assert.IsType<string>(race.Properties[RaceAssembler.PropStartFee]));
        Assert.Equal("SEK", Assert.IsType<string>(race.Properties[RaceAssembler.PropCurrency]));
    }

    [Fact]
    public async Task AssembleRacesAsync_GeocodesLocationOnlyMittloppDiscoveryIntoPointRace()
    {
        var geocoder = new FakeLocationGeocodingService(("Säter, Dalarna", "SE"), (60.35, 15.75));
        var doc = new RaceOrganizerDocument
        {
            Id = "sater-triathlon.se",
            Url = "https://sater-triathlon.se",
            Discovery = new Dictionary<string, List<SourceDiscovery>>(StringComparer.OrdinalIgnoreCase)
            {
                ["mittlopp"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Säter Triathlon – Olympisk / Tävling (STC)",
                        Date = "2026-07-11",
                        Distance = "1500 m swim, 40 km bike, 10 km run",
                        Country = "SE",
                        Location = "Säter, Dalarna",
                        RaceType = "triathlon",
                        ImageUrl = "https://mittlopp.se/Upload/CompHeader/ch_1562.jpg?min=38",
                        StartFee = "500 kr",
                        Currency = "SEK",
                        SourceUrls =
                        [
                            "https://mittlopp.se/anm/satertriathlon2026/PEYPFWREAWHIYOGCRA?lang=sv",
                            "http://www.sater-triathlon.se/"
                        ]
                    }
                ]
            }
        };

        var races = await RaceAssembler.AssembleRacesAsync(doc, geocoder, CancellationToken.None);

        var race = Assert.Single(races);
        var point = Assert.IsType<Point>(race.Geometry);
        Assert.Equal(15.75, point.Coordinates.Longitude);
        Assert.Equal(60.35, point.Coordinates.Latitude);
        Assert.Equal("http://www.sater-triathlon.se/", Assert.IsType<string>(race.Properties[RaceAssembler.PropWebsite]));
        Assert.Equal("500 kr", Assert.IsType<string>(race.Properties[RaceAssembler.PropStartFee]));
        Assert.Equal([("Säter, Dalarna", "SE")], geocoder.Requests);
    }

    [Fact]
    public async Task AssembleRacesAsync_DoesNotGeocodeWhenDiscoveryAlreadyHasCoordinates()
    {
        var geocoder = new FakeLocationGeocodingService(("Should Not Be Used", "SE"), (59.0, 18.0));
        var doc = new RaceOrganizerDocument
        {
            Id = "sample.org",
            Url = "https://sample.org",
            Discovery = new Dictionary<string, List<SourceDiscovery>>(StringComparer.OrdinalIgnoreCase)
            {
                ["mittlopp"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-30T00:00:00Z",
                        Name = "Sample Race",
                        Date = "2026-07-11",
                        Distance = "10 km",
                        Country = "SE",
                        Location = "Should Not Be Used",
                        Latitude = 60.1,
                        Longitude = 15.2,
                        RaceType = "running"
                    }
                ]
            }
        };

        var races = await RaceAssembler.AssembleRacesAsync(doc, geocoder, CancellationToken.None);

        var race = Assert.Single(races);
        var point = Assert.IsType<Point>(race.Geometry);
        Assert.Equal(15.2, point.Coordinates.Longitude);
        Assert.Equal(60.1, point.Coordinates.Latitude);
        Assert.Empty(geocoder.Requests);
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
