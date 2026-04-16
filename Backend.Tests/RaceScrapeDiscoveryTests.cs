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

        var jobs = RaceScrapeDiscovery.ParseUtmbRacePages(payload);

        Assert.Equal(2, jobs.Count);

        var utmb50k = Assert.Single(jobs, j => j.Url!.AbsoluteUri == "https://utmb.world/races/utmb-mont-blanc-50k");
        Assert.Equal("54.3 km", utmb50k.Distance);
        Assert.Equal(3200, utmb50k.ElevationGain);
        Assert.Equal("FR", utmb50k.Country);
        Assert.Equal("Chamonix", utmb50k.Location);
        Assert.Equal(["UTMB Mont-Blanc"], utmb50k.Playgrounds);
        Assert.Equal(["Finisher Stone"], utmb50k.RunningStones);
        Assert.Equal("https://utmb.world/img/race.jpg", utmb50k.ImageUrl);

        var ccc = Assert.Single(jobs, j => j.Url!.AbsoluteUri == "https://utmb.world/races/ccc");
        Assert.Equal("101 km", ccc.Distance);
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
        Assert.Equal("https://www.vmxtreme.se/", marker.Url!.AbsoluteUri);
        Assert.Equal("Vånga Mountain Xtreme - VMX", marker.Name);
        Assert.Equal(56.1774298686757, marker.Latitude);
        Assert.Equal(14.3645238871977, marker.Longitude);
        Assert.Equal("20250914", marker.Date);
        Assert.Equal("trail", marker.RaceType);
        Assert.Equal("Trail", marker.TypeLocal);
        Assert.Equal("Skåne", marker.County);
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

    [Theory]
    [InlineData("Marathon", "42 km")]
    [InlineData("marathon", "42 km")]
    [InlineData("MARATHON", "42 km")]
    [InlineData("Halvmarathon", "21 km")]
    [InlineData("halvmarathon", "21 km")]
    [InlineData("Half marathon", "21 km")]
    [InlineData("half marathon", "21 km")]
    [InlineData("Half-marathon", "21 km")]
    public void ParseDistanceVerbose_TranslatesMarathonKeywords(string input, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.ParseDistanceVerbose(input));
    }

    [Theory]
    [InlineData("10K, Marathon", "10 km, 42 km")]
    [InlineData("Halvmarathon, 10k", "21 km, 10 km")]
    [InlineData("Half marathon, Marathon", "21 km, 42 km")]
    public void ParseDistanceVerbose_TranslatesMarathonKeywordsInCombinedString(string input, string expected)
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

    // ── MatchDistanceKmToVerbose ──────────────────────────────────────────────

    [Theory]
    [InlineData(34.5, "34.2 km, 12.9 km", "34.2 km")]   // close to first entry
    [InlineData(13.0, "34.2 km, 12.9 km", "12.9 km")]   // close to second entry
    [InlineData(100.0, "100 km, 50 km", "100 km")]        // exact match
    [InlineData(50.0, "100 km, 50 km", "50 km")]          // exact match second
    public void MatchDistanceKmToVerbose_ReturnsClosestMatch(double gpxKm, string verbose, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.MatchDistanceKmToVerbose(gpxKm, verbose));
    }

    [Theory]
    [InlineData(50.0, null)]
    [InlineData(50.0, "")]
    [InlineData(0.0, "50 km")]
    public void MatchDistanceKmToVerbose_ReturnsNullForMissingInput(double gpxKm, string? verbose)
    {
        Assert.Null(RaceScrapeDiscovery.MatchDistanceKmToVerbose(gpxKm, verbose));
    }

    [Fact]
    public void MatchDistanceKmToVerbose_ReturnsNullWhenNoMatchWithinTolerance()
    {
        // 50 km GPX vs 100 km verbose — 50 % difference, exceeds 25 % tolerance.
        Assert.Null(RaceScrapeDiscovery.MatchDistanceKmToVerbose(50.0, "100 km"));
    }

    [Theory]
    [InlineData(42.0, "Marathon", "42 km")]     // exact marathon match
    [InlineData(42.2, "Marathon", "42 km")]     // slightly over marathon distance (within 25%)
    [InlineData(21.0, "Halvmarathon", "21 km")] // halvmarathon
    [InlineData(21.1, "Half marathon", "21 km")]// half marathon
    [InlineData(42.0, "10 km, Marathon", "42 km")]  // marathon in multi-distance list
    [InlineData(10.0, "10 km, Marathon", "10 km")]  // picks shorter distance from same list
    public void MatchDistanceKmToVerbose_HandlesMarathonKeywords(double gpxKm, string verbose, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.MatchDistanceKmToVerbose(gpxKm, verbose));
    }

    // ── ExtractCourseLinksFromHtml ────────────────────────────────────────────

    [Fact]
    public void ExtractCourseLinksFromHtml_FindsLinksByHrefKeyword()
    {
        const string html = """
            <html>
              <body>
                <a href="/course/info">Race information</a>
                <a href="/about">About us</a>
                <a href="/lopp/10k">10K race</a>
              </body>
            </html>
            """;

        var links = RaceScrapeDiscovery.ExtractCourseLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/course/info");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/lopp/10k");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("about"));
    }

    [Fact]
    public void ExtractCourseLinksFromHtml_FindsLinksByLinkText()
    {
        const string html = """
            <html>
              <body>
                <a href="/page1">Läs mer om loppet</a>
                <a href="/page2">See more info</a>
                <a href="/page3">Contact us</a>
                <a href="/page4">Info om banan</a>
              </body>
            </html>
            """;

        var links = RaceScrapeDiscovery.ExtractCourseLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/page1");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/page2");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/page4");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("page3"));
    }

    [Fact]
    public void ExtractCourseLinksFromHtml_ReturnsEmptyForBlankInput()
    {
        Assert.Empty(RaceScrapeDiscovery.ExtractCourseLinksFromHtml("", new Uri("https://example.com/")));
    }

    // ── ExtractGpxLinksFromHtml ───────────────────────────────────────────────

    [Fact]
    public void ExtractGpxLinksFromHtml_FindsGpxByExtensionAndByLinkText()
    {
        const string html = """
            <html>
              <body>
                <a href="/routes/race.gpx">Download route</a>
                <a href="/download">Download GPX file</a>
                <a href="/info">Race information</a>
              </body>
            </html>
            """;

        var links = RaceScrapeDiscovery.ExtractGpxLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/routes/race.gpx");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/download");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("info"));
    }

    [Fact]
    public void ExtractGpxLinksFromHtml_DeduplicatesResults()
    {
        const string html = """
            <html>
              <body>
                <a href="/race.gpx">Download GPX</a>
                <a href="/race.gpx">GPX fil</a>
              </body>
            </html>
            """;

        var links = RaceScrapeDiscovery.ExtractGpxLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Single(links, u => u.AbsoluteUri == "https://example.com/race.gpx");
    }

    // ── ExtractDownloadLinksFromHtml ──────────────────────────────────────────

    [Fact]
    public void ExtractDownloadLinksFromHtml_FindsSwedishAndEnglishDownloadLinks()
    {
        const string html = """
            <html>
              <body>
                <a href="/dl1">Ladda ner</a>
                <a href="/dl2">Hämta filen</a>
                <a href="/dl3">Download file</a>
                <a href="/about">About us</a>
              </body>
            </html>
            """;

        var links = RaceScrapeDiscovery.ExtractDownloadLinksFromHtml(html, new Uri("https://example.com/"));

        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/dl1");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/dl2");
        Assert.Contains(links, u => u.AbsoluteUri == "https://example.com/dl3");
        Assert.DoesNotContain(links, u => u.AbsoluteUri.Contains("about"));
    }

    // ── AssignDistancesToRoutes ───────────────────────────────────────────────

    [Fact]
    public void AssignDistancesToRoutes_MatchesEachRouteToClosestVerboseDistance()
    {
        var assignments = RaceScrapeDiscovery.AssignDistancesToRoutes([10.3, 40.1], "10 km, 40 km");

        Assert.Equal(2, assignments.Count);
        Assert.Equal(["10 km"], assignments[0]);
        Assert.Equal(["40 km"], assignments[1]);
    }

    [Fact]
    public void AssignDistancesToRoutes_AssignsUnmatchedDistancesToClosestRoute()
    {
        // Routes at ~10 km and ~40 km; verbose distances are 10, 20, 40 km.
        // 20 km is unmatched (outside 25% tolerance of both routes), so it goes to the closest: 10 km route.
        var assignments = RaceScrapeDiscovery.AssignDistancesToRoutes([10.3, 40.1], "10 km, 20 km, 40 km");

        Assert.Equal(2, assignments.Count);
        Assert.Contains("10 km", assignments[0]);
        Assert.Contains("20 km", assignments[0]);
        Assert.Equal(["40 km"], assignments[1]);
    }

    [Fact]
    public void AssignDistancesToRoutes_PrimaryDistancesAppearFirst()
    {
        // Route at ~10 km — "10 km" is primary (within tolerance), "20 km" is overflow.
        var assignments = RaceScrapeDiscovery.AssignDistancesToRoutes([10.3, 40.1], "10 km, 20 km, 40 km");

        Assert.Equal("10 km", assignments[0][0]);
        Assert.Equal("20 km", assignments[0][1]);
    }

    [Fact]
    public void AssignDistancesToRoutes_ReturnsEmptyListsForNullVerbose()
    {
        var assignments = RaceScrapeDiscovery.AssignDistancesToRoutes([10.0, 40.0], null);

        Assert.All(assignments, list => Assert.Empty(list));
    }

    [Fact]
    public void AssignDistancesToRoutes_HandlesMarathonKeyword()
    {
        // Route at ~42 km should match "Marathon" verbose distance.
        var assignments = RaceScrapeDiscovery.AssignDistancesToRoutes([42.1, 10.0], "Marathon, 10 km");

        Assert.Contains("42 km", assignments[0]);
        Assert.Contains("10 km", assignments[1]);
    }

    [Fact]
    public void AssignDistancesToRoutes_ReturnsEmptyListsForEmptyRoutes()
    {
        var assignments = RaceScrapeDiscovery.AssignDistancesToRoutes([], "10 km, 20 km");

        Assert.Empty(assignments);
    }

    // ── ParseTraceDeTrailCalendarEvents ───────────────────────────────────────

    [Fact]
    public void ParseTraceDeTrailCalendarEvents_EmitsItraJobsAndEventPageJobs()
    {
        const string payload = """
            {
              "success": 1,
              "data": [
                {
                  "nom": "Ultra Tour 4 Massifs",
                  "traceIDs": "12345_67890",
                  "distances": "50_100",
                  "country": "FR",
                  "label": "ultra-tour-4-massifs",
                  "sports": "trail",
                  "img": "race.jpg",
                  "logo": null
                },
                {
                  "nom": "Another Race",
                  "traceIDs": "11111",
                  "distances": "42",
                  "country": "IT",
                  "label": "another-race",
                  "sports": "trail running",
                  "img": null,
                  "logo": "logo.jpg"
                }
              ]
            }
            """;

        var jobs = RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(payload);

        // ITRA jobs: one per trace ID
        var itraJobs = jobs.Where(j => j.Url!.AbsoluteUri.Contains("/trace/getTraceItra/")).ToList();
        Assert.Equal(3, itraJobs.Count);

        var trace12345 = Assert.Single(itraJobs, j => j.Url!.AbsoluteUri.EndsWith("/12345"));
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/12345", trace12345.Url!.AbsoluteUri);
        Assert.Equal("50 km", trace12345.Distance);
        Assert.Equal("Ultra Tour 4 Massifs", trace12345.Name);
        Assert.Equal("FR", trace12345.Country);
        Assert.Equal("https://tracedetrail.fr/events/race.jpg", trace12345.ImageUrl);

        var trace67890 = Assert.Single(itraJobs, j => j.Url!.AbsoluteUri.EndsWith("/67890"));
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/67890", trace67890.Url!.AbsoluteUri);
        Assert.Equal("100 km", trace67890.Distance);

        var trace11111 = Assert.Single(itraJobs, j => j.Url!.AbsoluteUri.EndsWith("/11111"));
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/11111", trace11111.Url!.AbsoluteUri);
        Assert.Equal("42 km", trace11111.Distance);
        // logo fallback when img is null
        Assert.Equal("https://tracedetrail.fr/events/logo.jpg", trace11111.ImageUrl);

        // Event page jobs: one per unique slug
        var eventPageJobs = jobs.Where(j => j.Url!.AbsoluteUri.Contains("/en/event/")).ToList();
        Assert.Equal(2, eventPageJobs.Count);

        var eventJob1 = Assert.Single(eventPageJobs, j => j.Url!.AbsoluteUri.Contains("ultra-tour-4-massifs"));
        Assert.Equal("https://tracedetrail.fr/en/event/ultra-tour-4-massifs", eventJob1.Url!.AbsoluteUri);
        Assert.Equal("50 km, 100 km", eventJob1.Distance);

        var eventJob2 = Assert.Single(eventPageJobs, j => j.Url!.AbsoluteUri.Contains("another-race"));
        Assert.Equal("https://tracedetrail.fr/en/event/another-race", eventJob2.Url!.AbsoluteUri);
        Assert.Equal("42 km", eventJob2.Distance);
    }

    [Fact]
    public void ParseTraceDeTrailCalendarEvents_ReturnsEmptyForBlankInput()
    {
        Assert.Empty(RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(""));
        Assert.Empty(RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents("{}"));
    }

    // ── BuildFeatureId (URL overload) ─────────────────────────────────────────

    [Theory]
    [InlineData("https://julianalps.utmb.world/races/120K", null, "julianalps.utmb.world-races-120K")]
    [InlineData("https://www.vmxtreme.se/", null, "vmxtreme.se")]
    [InlineData("https://tracedetrail.fr/trace/getTraceItra/12345", null, "tracedetrail.fr-trace-getTraceItra-12345")]
    [InlineData("https://julianalps.utmb.world/races/120K", 0, "julianalps.utmb.world-races-120K-0")]
    [InlineData("https://julianalps.utmb.world/races/120K", 1, "julianalps.utmb.world-races-120K-1")]
    public void BuildFeatureId_FromUrl_BuildsCosmosId(string url, int? routeIndex, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.BuildFeatureId(new Uri(url), routeIndex));
    }

    // ── BuildFeatureId (name+distance overload) ───────────────────────────────

    [Theory]
    [InlineData("Race Name", "50 km", "race-name-50-km")]
    [InlineData("Race Name", null, "race-name")]
    [InlineData(null, "50 km", "50-km")]
    public void BuildFeatureId_FromNameAndDistance_BuildsSlug(string? name, string? distance, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.BuildFeatureId(name, distance));
    }

    // ── ExtractRaceSiteUrl ────────────────────────────────────────────────────

    [Fact]
    public void ExtractRaceSiteUrl_FindsSiteDelaCourseLink()
    {
        const string html = """
            <html>
              <body>
                <a href="https://utmb.world/races/120K">UTMB</a>
                <a href="https://myrace.com/">Site de la course</a>
                <a href="/info">Info</a>
              </body>
            </html>
            """;

        var result = RaceScrapeDiscovery.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/some-event"));
        Assert.Equal("https://myrace.com/", result?.AbsoluteUri);
    }

    [Fact]
    public void ExtractRaceSiteUrl_IsCaseInsensitive()
    {
        const string html = """<a href="https://myrace.com/">SITE DE LA COURSE</a>""";
        var result = RaceScrapeDiscovery.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/x"));
        Assert.Equal("https://myrace.com/", result?.AbsoluteUri);
    }

    [Fact]
    public void ExtractRaceSiteUrl_ReturnsNullWhenNoneFound()
    {
        const string html = "<html><body><a href='/info'>Info</a></body></html>";
        Assert.Null(RaceScrapeDiscovery.ExtractRaceSiteUrl(html, new Uri("https://tracedetrail.fr/en/event/x")));
    }

    [Fact]
    public void ExtractRaceSiteUrl_ReturnsNullForBlankInput()
    {
        Assert.Null(RaceScrapeDiscovery.ExtractRaceSiteUrl("", new Uri("https://tracedetrail.fr/en/event/x")));
    }
}
