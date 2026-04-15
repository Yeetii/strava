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
}
