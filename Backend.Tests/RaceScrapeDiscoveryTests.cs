using Backend;

namespace Backend.Tests;

public class RaceScrapeDiscoveryTests
{
    [Fact]
    public void ParseUtmbRacePages_ExtractsRacePagesAndMetadata()
    {
        const string payload = """
            {
              "races": [
                {
                  "slug": "https://utmb.world/races/utmb-mont-blanc-50k",
                  "name": "UTMB Mont-Blanc 50K",
                  "details": {
                    "statsUp": [
                      { "name": "distance", "value": 54.3 },
                      { "name": "elevationGain", "value": 3200 }
                    ]
                  },
                  "country": "FR",
                  "city": "Chamonix",
                  "playgrounds": [{ "name": "UTMB Mont-Blanc" }],
                  "runningStones": [{ "name": "Finisher Stone" }],
                  "image": "https://utmb.world/img/race.jpg"
                },
                {
                  "slug": "https://utmb.world/races/ccc",
                  "name": "CCC",
                  "details": {
                    "statsUp": [
                      { "name": "distance", "value": 101.0 },
                      { "name": "elevationGain", "value": 6100 }
                    ]
                  }
                }
              ]
            }
            """;

        var pages = RaceScrapeDiscovery.ParseUtmbRacePages(payload);

        Assert.Equal(2, pages.Count);

        var utmb50k = Assert.Single(pages, p => p.PageUrl.AbsoluteUri == "https://utmb.world/races/utmb-mont-blanc-50k");
        Assert.Equal(54.3, utmb50k.Distance);
        Assert.Equal(3200, utmb50k.ElevationGain);
        Assert.Equal("FR", utmb50k.Country);
        Assert.Equal("Chamonix", utmb50k.Location);
        Assert.Equal(["UTMB Mont-Blanc"], utmb50k.Playgrounds);
        Assert.Equal(["Finisher Stone"], utmb50k.RunningStones);
        Assert.Equal("https://utmb.world/img/race.jpg", utmb50k.ImageUrl);

        var ccc = Assert.Single(pages, p => p.PageUrl.AbsoluteUri == "https://utmb.world/races/ccc");
        Assert.Equal(101.0, ccc.Distance);
        Assert.Equal(6100, ccc.ElevationGain);
        Assert.Null(ccc.Playgrounds);
    }

    [Fact]
    public void ExtractGpxUrlsFromHtml_FindsLinksFromHrefAndEscapedScriptUrls()
    {
        const string html = """
            <html>
              <body>
                <a href="/courses/utmb-50k.gpx">Download GPX</a>
                <script>
                  window.courseData = {"gpx":"https:\/\/utmb.world\/downloads\/ccc-course.gpx?download=1"};
                </script>
              </body>
            </html>
            """;

        var urls = RaceScrapeDiscovery.ExtractGpxUrlsFromHtml(html, new Uri("https://utmb.world/races/ccc"));

        Assert.Contains(urls, uri => uri.AbsoluteUri == "https://utmb.world/courses/utmb-50k.gpx");
        Assert.Contains(urls, uri => uri.AbsoluteUri == "https://utmb.world/downloads/ccc-course.gpx?download=1");
    }

    [Fact]
    public void ParseLoppkartanMarkers_ExtractsValidMarkers()
    {
        const string payload = """
            {
              "generatedAt": "2026-04-09T08:56:43.950Z",
              "country": "se",
              "markers": [
                {
                  "id": "eb3555a8-38ab-43df-b094-ff01d8d27000",
                  "domain_name": "vanga_mountain_xtreme",
                  "name": "Vånga Mountain Xtreme - VMX",
                  "location": "Vångabacken",
                  "county": "Skåne",
                  "latitude": 56.1774298686757,
                  "longitude": 14.3645238871977,
                  "website": "https://www.vmxtreme.se/",
                  "race_date": "20250914",
                  "race_type": "trail",
                  "type_local": "Trail",
                  "origin_country": "se",
                  "distance_verbose": null
                },
                {
                  "id": "eb3555a8-38ab-43df-b094-ff01d8d27000",
                  "latitude": 56.17,
                  "longitude": 14.36
                },
                {
                  "id": "bad-marker",
                  "latitude": "not-a-number",
                  "longitude": 14.0
                }
              ]
            }
            """;

        var markers = RaceScrapeDiscovery.ParseLoppkartanMarkers(payload);

        var marker = Assert.Single(markers);
        Assert.Equal("eb3555a8-38ab-43df-b094-ff01d8d27000", marker.MarkerId);
        Assert.Equal("Vånga Mountain Xtreme - VMX", marker.Name);
        Assert.Equal(56.1774298686757, marker.Latitude);
        Assert.Equal(14.3645238871977, marker.Longitude);
        Assert.Equal("https://www.vmxtreme.se/", marker.Website);
        Assert.Equal("20250914", marker.RaceDate);
        Assert.Equal("trail", marker.RaceType);
        Assert.Equal("Trail", marker.TypeLocal);
        Assert.Equal("vanga_mountain_xtreme", marker.DomainName);
    }

    // ── NormalizeDateToYyyyMmDd ────────────────────────────────────────────────

    [Theory]
    [InlineData("20250914", "2025-09-14")]
    [InlineData("2025-09-14", "2025-09-14")]
    [InlineData("2025-01-01", "2025-01-01")]
    [InlineData("20231231", "2023-12-31")]
    public void NormalizeDateToYyyyMmDd_ConvertsKnownFormats(string input, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(input));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void NormalizeDateToYyyyMmDd_ReturnsNullForBlankInput(string? input)
    {
        Assert.Null(RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(input));
    }

    // ── FormatDistanceKm ──────────────────────────────────────────────────────

    [Theory]
    [InlineData(5.0, "5 km")]
    [InlineData(10.1, "10.1 km")]
    [InlineData(100.0, "100 km")]
    [InlineData(42.195, "42.2 km")]
    public void FormatDistanceKm_FormatsCorrectly(double input, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.FormatDistanceKm(input));
    }

    // ── ParseDistanceVerbose ──────────────────────────────────────────────────

    [Theory]
    [InlineData("100K", "100 km")]
    [InlineData("100k, 50k, 25k", "100 km, 50 km, 25 km")]
    [InlineData("100km", "100 km")]
    [InlineData("10.5K", "10.5 km")]
    [InlineData(null, null)]
    [InlineData("", null)]
    public void ParseDistanceVerbose_NormalisesDistanceStrings(string? input, string? expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.ParseDistanceVerbose(input));
    }

    // ── NormalizeCountryToIso2 ────────────────────────────────────────────────

    [Theory]
    [InlineData("se", "SE")]
    [InlineData("SE", "SE")]
    [InlineData("SWE", "SE")]
    [InlineData("FRA", "FR")]
    [InlineData("france", "FR")]
    [InlineData("France", "FR")]
    [InlineData("suède", "SE")]
    [InlineData("germany", "DE")]
    [InlineData("allemagne", "DE")]
    [InlineData(null, null)]
    [InlineData("", null)]
    public void NormalizeCountryToIso2_NormalizesKnownValues(string? input, string? expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.NormalizeCountryToIso2(input));
    }

    // ── NormalizeRaceType ─────────────────────────────────────────────────────

    [Theory]
    [InlineData("trail", "trail")]
    [InlineData("Trail", "trail")]
    [InlineData("randotrail", "trail, randotrail")]
    [InlineData("RandoTrail", "trail, randotrail")]
    [InlineData("Trail running", "trail, trail running")]
    [InlineData("marathon", "marathon")]
    [InlineData("Trail, marathon", "trail, marathon")]
    [InlineData(null, null)]
    [InlineData("", null)]
    public void NormalizeRaceType_ConvertsRaceTypesCorrectly(string? input, string? expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.NormalizeRaceType(input));
    }

    [Fact]
    public void NormalizeRaceType_DeduplicatesTokens()
    {
        var result = RaceScrapeDiscovery.NormalizeRaceType("trail, Trail, marathon");
        Assert.Equal("trail, marathon", result);
    }
}
