using Backend;

namespace Backend.Tests;

public class RaceScrapeDiscoveryTests
{
    [Fact]
    public void ParseUtmbRacePages_ExtractsRacePagesAndMetadata()
    {
        const string payload = """
            {
              "results": [
                {
                  "name": "UTMB Mont-Blanc 50K",
                  "distance": 54.3,
                  "elevationGain": 3200,
                  "websiteUrl": "https://utmb.world/races/utmb-mont-blanc-50k"
                },
                {
                  "name": "CCC",
                  "distanceKm": 101.0,
                  "dPlus": 6100,
                  "url": "/races/ccc"
                }
              ]
            }
            """;

        var pages = RaceScrapeDiscovery.ParseUtmbRacePages(payload);

        Assert.Equal(2, pages.Count);
        Assert.Contains(pages, p =>
            p.PageUrl.AbsoluteUri == "https://utmb.world/races/utmb-mont-blanc-50k"
            && p.Distance == 54.3
            && p.ElevationGain == 3200);
        Assert.Contains(pages, p =>
            p.PageUrl.AbsoluteUri == "https://utmb.world/races/ccc"
            && p.Distance == 101.0
            && p.ElevationGain == 6100);
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
}
