using Backend;
using Shared.Services;
using System.Net;
using System.Net.Http;

namespace Backend.Tests;

public class RaceScrapeDiscoveryTests
{
    [Fact]
    public void ParseUtmbRacePages_ExtractsRacePagesAndMetadata()
    {
        const string payload = """
            {"races":[
            {
                "id": 133,
                "startDate": "24th April 2026",
                "startLocation": "Malaucène, France",
                "raceStatus": {
                    "open": false,
                    "status": "registration_sold_out"
                },
                "name": "Grand Raid Ventoux by UTMB - Ultra Géant de Provence - UGP",
                "logo": null,
                "eventLogo": {
                    "publicId": "/ventoux/Logos/Logo_GRAND_RAID_VENTOUX_white_346cd68fd0.png"
                },
                "media": {
                    "publicId": "ventoux/Races/2025/grv25_100_M_Collectif_Com_D_Rosso_7853_92afa1b124",
                    "ratio": 1.5,
                    "format": "jpg",
                    "type": "image",
                    "width": 1920,
                    "height": 1280
                },
                "slug": "https://ventoux.utmb.world/races/GRV100M",
                "raceLink": null,
                "details": {
                    "summaryUp": null,
                    "summaryDown": null,
                    "statsUp": [
                    {
                        "name": "distance",
                        "value": 125,
                        "postfix": "km"
                    },
                    {
                        "name": "elevationGain",
                        "value": 5700,
                        "postfix": "m"
                    },
                    {
                        "name": "runningStones",
                        "value": 4,
                        "postfix": null
                    },
                    {
                        "name": "categoryWorldSeries",
                        "value": "100m",
                        "postfix": null
                    }
                    ],
                    "statsDown": [
                    {
                        "name": "startPlace",
                        "value": "Malaucène, France",
                        "postfix": null
                    }
                    ]
                },
                "raceTheme": "#f42525",
                "raceThemeIsDark": true,
                "playgrounds": [
                    "hikingTrail"
                ]
            }
            ]}
            """;

        var jobs = RaceScrapeDiscovery.ParseUtmbRacePages(payload);

        Assert.Single(jobs);

        var GRV100M = Assert.Single(jobs, j => j.UtmbUrl!.AbsoluteUri == "https://ventoux.utmb.world/races/GRV100M");
        Assert.Equal("125 km", GRV100M.Distance);
        Assert.Equal(5700, GRV100M.ElevationGain);
        Assert.Equal("Grand Raid Ventoux by UTMB - Ultra Géant de Provence - UGP", GRV100M.Name);
        Assert.Equal(new Dictionary<string, string> { ["utmb"] = "133" }, GRV100M.ExternalIds);
        Assert.Equal("2026-04-24", GRV100M.Date);
        Assert.Equal("FR", GRV100M.Country);
        Assert.Equal("Malaucène", GRV100M.Location);
        Assert.Equal(false, GRV100M.RegistrationOpen);
        Assert.Equal(["hikingTrail"], GRV100M.Playgrounds);
        Assert.Equal(4, GRV100M.RunningStones);
        Assert.Equal("100m", GRV100M.UtmbWorldSeriesCategory);
        Assert.Equal("https://res.cloudinary.com/utmb-world/image/upload/ventoux/Races/2025/grv25_100_M_Collectif_Com_D_Rosso_7853_92afa1b124", GRV100M.ImageUrl);
        Assert.Equal("https://res.cloudinary.com/utmb-world/image/upload/ventoux/Logos/Logo_GRAND_RAID_VENTOUX_white_346cd68fd0.png", GRV100M.LogoUrl);
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
        Assert.Equal("https://www.vmxtreme.se/", marker.WebsiteUrl!.AbsoluteUri);
        Assert.Equal("Vånga Mountain Xtreme - VMX", marker.Name);
        Assert.Equal(56.1774298686757, marker.Latitude);
        Assert.Equal(14.3645238871977, marker.Longitude);
        Assert.Equal("20250914", marker.Date);
        Assert.Equal("trail", marker.RaceType);
        Assert.Equal("Trail", marker.TypeLocal);
        Assert.Equal("Skåne", marker.County);
    }

    [Fact]
    public void ExtractItraRequestVerificationToken_ReturnsTokenFromHtml()
    {
        const string html = "<input name=\"__RequestVerificationToken\" type=\"hidden\" value=\"abc123\" />";

        var token = ItraDiscoveryAgent.ExtractRequestVerificationToken(html);

        Assert.Equal("abc123", token);
    }

    [Fact]
    public void ParseItraCalendarPage_ExtractsRaceJobsFromEmbeddedFragments()
    {
        const string html = """
            <script>
            var raceSearchJsonSidePopupNew = [
            "<div class='popupNew'><div class='event_name'><a href='/Races/RaceDetails/1234'>Some Race</a></div><h4>Sample ITRA Race</h4><div class='date'>10.04.2026</div><div class='location'>Foo Town, Spain</div><div class='count'>42.2 km</div></div>"
            ];
            </script>
            """;

        var jobs = ItraDiscoveryAgent.ParseCalendarPage(html, new Uri("https://itra.run/Races/RaceCalendar"));

        var job = Assert.Single(jobs);
        Assert.Equal("Sample ITRA Race", job.Name);
        Assert.Equal("2026-04-10", job.Date);
        Assert.Equal("ES", job.Country);
        Assert.Equal("Foo Town", job.Location);
        Assert.Equal("42.2 km", job.Distance);
        Assert.Equal("https://itra.run/Races/RaceDetails/1234", job.WebsiteUrl!.AbsoluteUri);
        Assert.Equal(new Dictionary<string, string> { ["itraEventId"] = "1234" }, job.ExternalIds);
        Assert.Equal("trail", job.RaceType);
    }

    [Fact]
    public void ExtractItraPoints_ParsesIconDigitSequenceAnywhereOnPage()
    {
        const string html = """
            <div>
                <div class="row">
                    <div class="col"><h5>ITRA Points</h5></div>
                </div>
                <div class="row">
                    <div class="col"><img src="/images/itra_numbers/icons/one.svg" alt="Itra Images" /></div>
                    <div class="col"><img src="/images/itra_numbers/icons/two.svg" alt="Itra Images" /></div>
                </div>
            </div>
            """;

        var points = RaceHtmlScraper.ExtractItraPoints(html);

        Assert.Equal(12, points);
    }

    [Fact]
    public void ParseItraCalendarPage_SplitsMultiRaceButtonGroupsIntoSeparateJobs()
    {
        const string html = """
            <script>
            var raceSearchJsonSidePopupNew = [
            "<div class='popupNew'><div class='event_name'><a href='/Races/RaceDetails/112171'>Some Race Group</a></div><h4>Sample ITRA Event Group</h4><div class='date'>10.04.2026</div><div class='location'>Foo Town, Spain</div><div class='btn-group'><a href='/Races/RaceDetails/first'>First Race</a><a href='/Races/RaceDetails/second'>Second Race</a></div></div>"
            ];
            </script>
            """;

        var jobs = ItraDiscoveryAgent.ParseCalendarPage(html, new Uri("https://itra.run/Races/RaceCalendar"));

        Assert.Equal(2, jobs.Count);
        Assert.Contains(jobs, j => j.WebsiteUrl!.AbsoluteUri == "https://itra.run/Races/RaceDetails/first" && j.Name == "First Race");
        Assert.Contains(jobs, j => j.WebsiteUrl!.AbsoluteUri == "https://itra.run/Races/RaceDetails/second" && j.Name == "Second Race");
    }

    [Fact]
    public void EnrichItraJobFromEventPageHtml_UsesRegisterLinkAndParsesH1Description()
    {
        const string html = """
            <html>
              <body>
                <h1>Sample ITRA Race</h1>
                <div class="col-lg-9">This is the race description.</div>
                <div>Elevation Gain: 1200 m</div>
                <div>National League: yes</div>
                <img src="itra_pts_race3.png" />
                <a href="/Races/Register/9876">Register to this race</a>
              </body>
            </html>
            """;

        var job = new ScrapeJob(WebsiteUrl: new Uri("https://itra.run/Races/RaceDetails/1234"));
        var enriched = ItraDiscoveryAgent.EnrichJobFromEventPageHtml(job, html, job.WebsiteUrl!);

        Assert.Equal("https://itra.run/Races/Register/9876", enriched.WebsiteUrl!.AbsoluteUri);
        Assert.Equal("https://itra.run/Races/RaceDetails/1234", enriched.ItraEventPageUrl!.AbsoluteUri);
        Assert.True(enriched.ItraNationalLeague);
        Assert.Equal("Sample ITRA Race", enriched.Name);
        Assert.Equal("This is the race description.", enriched.Description);
        Assert.Equal(1200, enriched.ElevationGain);
        Assert.Equal(3, enriched.ItraPoints);
        Assert.Equal("trail", enriched.RaceType);
    }

    [Fact]
    public void EnrichItraJobFromEventPageHtml_UsesRaceDateFromRaceDateSection()
    {
        const string html = """
            <html>
              <body>
                <h1>Sample ITRA Race</h1>
                <div class="row">
                  <div class="col-6 col-sm-3 p-2">
                    <i class="fas fa-calendar"></i>&nbsp;Race Date: <span style="font-weight:bold">2026/04/25</span>
                  </div>
                </div>
                <a href="/Races/Register/9876">Register to this race</a>
              </body>
            </html>
            """;

        var job = new ScrapeJob(WebsiteUrl: new Uri("https://itra.run/Races/RaceDetails/1234"), Date: "2026-04-10");
        var enriched = ItraDiscoveryAgent.EnrichJobFromEventPageHtml(job, html, job.WebsiteUrl!);

        Assert.Equal("2026-04-25", enriched.Date);
    }

    [Fact]
    public void EnrichItraJobFromEventPageHtml_UsesH3AndAboutDescriptionAndPageDistance()
    {
        const string html = """
            <html>
              <body>
                <div class="btn-group" role="group">
                  <a href="/Races/RaceDetails/first">First Race</a>
                  <a href="/Races/RaceDetails/second">Second Race</a>
                </div>
                <h3>Hardy Rolling</h3>
                <div class="tab-pane active">
                  <h3>About the Race</h3>
                  <div class="col-12">
                    <p>First race page description.</p>
                  </div>
                  <div>
                    <i class="fas fa-route"></i>&nbsp;Distance: <span>13.5</span>
                  </div>
                </div>
                <a href="/Races/Register/9876">Register to this race</a>
              </body>
            </html>
            """;

        var job = new ScrapeJob(WebsiteUrl: new Uri("https://itra.run/Races/RaceDetails/112171"), Distance: "100 km");
        var enriched = ItraDiscoveryAgent.EnrichJobFromEventPageHtml(job, html, job.WebsiteUrl!);

        Assert.Equal("Hardy Rolling", enriched.Name);
        Assert.Equal("First race page description.", enriched.Description);
        Assert.Equal("13.5 km", enriched.Distance);
        Assert.Equal("https://itra.run/Races/Register/9876", enriched.WebsiteUrl!.AbsoluteUri);
    }

    [Fact]
    public void ExtractItraPoints_ParsesIconDigitSequence()
    {
        const string html = """
            <div>
                <h5>ITRA Points</h5>
                <div class="row">
                    <div class="col">
                        <img src="/images/itra_numbers/icons/one.svg" alt="Itra Images" />
                        <img src="/images/itra_numbers/icons/two.svg" alt="Itra Images" />
                        <img src="/images/itra_numbers/icons/three.svg" alt="Itra Images" />
                    </div>
                </div>
            </div>
            """;

        var points = RaceHtmlScraper.ExtractItraPoints(html);

        Assert.Equal(123, points);
    }

    [Fact]
    public void ItraScrapeJob_ToSourceDiscovery_IncludesItraNationalLeague()
    {
        var job = new ScrapeJob(
            WebsiteUrl: new Uri("https://itra.run/Races/Register/9876"),
            ItraEventPageUrl: new Uri("https://itra.run/Races/RaceDetails/1234"),
            ItraNationalLeague: true);

        var discovery = job.ToSourceDiscovery();

        Assert.True(discovery.ItraNationalLeague);
        Assert.Contains("https://itra.run/Races/RaceDetails/1234", discovery.SourceUrls!);
    }

    [Fact]
    public async Task EnrichEventPageDetailsAsync_ExpandsItraRaceButtonGroupLinks()
    {
        const string groupHtml = """
            <html>
              <body>
                <h1>Multi-Race Event</h1>
                <div class="col-lg-9">Group description.</div>
                <div class="row pt-2">
                  <div class="col">
                    <div class="row text-left">
                      <div class="col overflow-auto">
                        <div class="btn-group" role="group" aria-label="Basic example">
                          <a class="btn btn-outline-dark" href="/Races/RaceDetails/first">First Race</a>
                          <a class="btn btn-outline-dark" href="/Races/RaceDetails/second">Second Race</a>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </body>
            </html>
            """;

        const string firstHtml = """
            <html>
              <body>
                <h1>First Race</h1>
                <div class="col-lg-9">First race description.</div>
                <a href="/Races/Register/1">Register to this race</a>
              </body>
            </html>
            """;

        const string secondHtml = """
            <html>
              <body>
                <h1>Second Race</h1>
                <div class="col-lg-9">Second race description.</div>
                <a href="/Races/Register/2">Register to this race</a>
              </body>
            </html>
            """;

        var initialJob = new ScrapeJob(
            WebsiteUrl: new Uri("https://itra.run/Races/RaceDetails/group"),
            Date: "2026-04-25",
            Country: "PL",
            Location: "Foo Town",
            Distance: "14 km");
        using var client = new HttpClient(new MultiResponseHttpMessageHandler(
            new Dictionary<Uri, string>
            {
                [initialJob.WebsiteUrl!] = groupHtml,
                [new Uri("https://itra.run/Races/RaceDetails/first")] = firstHtml,
                [new Uri("https://itra.run/Races/RaceDetails/second")] = secondHtml,
            }));

        var enriched = await ItraDiscoveryAgent.EnrichEventPageDetailsAsync(new[] { initialJob }, client, CancellationToken.None);

        Assert.Equal(3, enriched.Count);
        Assert.Contains(enriched, j => j.Name == "Multi-Race Event");
        Assert.Contains(enriched, j => j.Name == "First Race");
        Assert.Contains(enriched, j => j.Name == "Second Race");
        Assert.Contains(enriched, j => j.WebsiteUrl!.AbsoluteUri == "https://itra.run/Races/Register/1");
        Assert.Contains(enriched, j => j.WebsiteUrl!.AbsoluteUri == "https://itra.run/Races/Register/2");

        var firstRace = Assert.Single(enriched, j => j.Name == "First Race");
        Assert.Equal("2026-04-25", firstRace.Date);
        Assert.Equal("PL", firstRace.Country);
        Assert.Equal("Foo Town", firstRace.Location);
    }

    [Fact]
    public void ParseDuvCalendarPage_ExtractsEventsFromHtml()
    {
        const string html = """
            <table width='100%' border='1' cellpadding='2' cellspacing='1'>
            <tr class='odd'>
              <td class='boxed' nowrap='nowrap' align='left'>15.-20.04.2026</td>
              <td class='boxed' align='left'><a href='eventdetail.php?event=129985'> South Wales 200</a></td>
              <td class='boxed' align='left'>100mi</td>
              <td class='boxed' align='left'>Chepstow (GBR)</td>
              <td class='boxed' align='left'>&nbsp;</td>
              <td class='boxed' align='left'>&nbsp; &nbsp;</td>
            </tr>
            </table>
            """;

        var jobs = RaceScrapeDiscovery.ParseDuvCalendarPage(html, new Uri("https://statistik.d-u-v.org/calendar.php"));

        var job = Assert.Single(jobs);
        Assert.Equal("South Wales 200", job.Name);
        Assert.Equal("160.9 km", job.Distance);
        Assert.Equal("2026-04-15", job.Date);
        Assert.Equal("GB", job.Country);
        Assert.Equal("Chepstow", job.Location);
        Assert.Equal("https://statistik.d-u-v.org/eventdetail.php?event=129985", job.WebsiteUrl!.AbsoluteUri);
        Assert.Equal(new Dictionary<string, string> { ["duv"] = "129985" }, job.ExternalIds);
    }

    [Fact]
    public void ExtractDuvEventWebPageUrl_ReturnsExternalWebsite()
    {
        const string html = """
            <tr><td align='right' valign='top'><b>Web page: </b></td>
                <td colspan='2'><a href='https://www.wildhorse200.com/south-wales-200/' target='_BLANK' rel='noopener'>https://www.wildhorse200.com/south-wales-200/</a></td></tr>
            """;

        var uri = RaceHtmlScraper.ExtractDuvEventWebPageUrl(html, new Uri("https://statistik.d-u-v.org/eventdetail.php?event=129985"));

        Assert.Equal("https://www.wildhorse200.com/south-wales-200/", uri!.AbsoluteUri);
    }

    [Fact]
    public void ExtractStartPositionUrl_ReturnsStartPosLinkFromEventDetailPage()
    {
        const string html = """
            <a href='startpos.php?event=129985'>Show start location</a>
            """;

        var url = DuvDiscoveryAgent.ExtractStartPositionUrl(html, new Uri("https://statistik.d-u-v.org/eventdetail.php?event=129985"));

        Assert.Equal("https://statistik.d-u-v.org/startpos.php?event=129985", url!.AbsoluteUri);
    }

    [Fact]
    public void ExtractStartPositionCoordinates_ParsesJavascriptLatLng()
    {
        const string html = """
            var RaceStart = new google.maps.LatLng(51.641857,-2.673804);
            """;

        var coords = DuvDiscoveryAgent.ExtractStartPositionCoordinates(html);

        Assert.Equal(51.641857, coords?.lat);
        Assert.Equal(-2.673804, coords?.lng);
    }

    [Fact]
    public async Task EnrichJobAsync_UsesStartPosPageCoordinates()
    {
        const string detailHtml = """
            <a href='startpos.php?event=129985'>Start location</a>
            <a href='https://www.wildhorse200.com/south-wales-200/'>Web page</a>
            """;
        const string startPosHtml = """
            var RaceStart = new google.maps.LatLng(51.641857,-2.673804);
            """;

        var job = new ScrapeJob(WebsiteUrl: new Uri("https://statistik.d-u-v.org/eventdetail.php?event=129985"));
        using var client = new HttpClient(new MultiResponseHttpMessageHandler(
            new Dictionary<Uri, string>
            {
                [job.WebsiteUrl!] = detailHtml,
                [new Uri("https://statistik.d-u-v.org/startpos.php?event=129985")] = startPosHtml,
            }));

        var enriched = await DuvDiscoveryAgent.EnrichJobAsync(client, job, CancellationToken.None);

        Assert.Equal("https://www.wildhorse200.com/south-wales-200/", enriched.WebsiteUrl!.AbsoluteUri);
        Assert.Equal(51.641857, enriched.Latitude);
        Assert.Equal(-2.673804, enriched.Longitude);
    }

    [Fact]
    public void EnrichJobFromEventDetailHtml_ExtractsDuvDetailMetadata()
    {
        const string detailHtml = """
            <tr><td align='right' valign='top'><b>Race type: </b></td><td colspan='2'>trail race</td></tr>
            <tr><td align='right' valign='top'><b>Elevation gain/loss: </b></td><td colspan='2'>  30,000ft </td></tr>
            <tr><td align='right' valign='top'><b>Course description: </b></td><td colspan='2'> One memorable and monstrous run across the trails...</td></tr>
            <tr><td align='right' valign='top'><b>Entry fee: </b></td><td colspan='2'> £399</td></tr>
            """;

        var job = new ScrapeJob(WebsiteUrl: new Uri("https://statistik.d-u-v.org/eventdetail.php?event=129985"));

        var enriched = DuvDiscoveryAgent.EnrichJobFromEventDetailHtml(job, detailHtml);

        Assert.Equal("trail race", enriched.RaceType);
        Assert.Equal(9144, enriched.ElevationGain);
        Assert.Equal("One memorable and monstrous run across the trails...", enriched.Description);
        Assert.Equal("£399", enriched.StartFee);
    }

    private sealed class MultiResponseHttpMessageHandler : HttpMessageHandler
    {
        private readonly Dictionary<Uri, string> _responses;

        public MultiResponseHttpMessageHandler(Dictionary<Uri, string> responses)
        {
            _responses = responses;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (!_responses.TryGetValue(request.RequestUri!, out var html))
                throw new InvalidOperationException($"Unexpected request URL: {request.RequestUri}");

            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(html)
            };
            return Task.FromResult(response);
        }
    }

    [Fact]
    public async Task EnrichJobAsync_UsesExternalWebsiteFromEventDetailPage()
    {
        const string html = """
            <tr><td align='right' valign='top'><b>Web page: </b></td>
                <td colspan='2'><a href='https://www.wildhorse200.com/south-wales-200/' target='_BLANK' rel='noopener'>https://www.wildhorse200.com/south-wales-200/</a></td></tr>
            """;

        var job = new ScrapeJob(WebsiteUrl: new Uri("https://statistik.d-u-v.org/eventdetail.php?event=129985"));
        using var client = new HttpClient(new StubHttpMessageHandler(job.WebsiteUrl!, html));

        var enriched = await DuvDiscoveryAgent.EnrichJobAsync(client, job, CancellationToken.None);

        Assert.Equal("https://www.wildhorse200.com/south-wales-200/", enriched.WebsiteUrl!.AbsoluteUri);
    }

    private sealed class StubHttpMessageHandler : HttpMessageHandler
    {
        private readonly Uri _expectedUri;
        private readonly string _html;

        public StubHttpMessageHandler(Uri expectedUri, string html)
        {
            _expectedUri = expectedUri;
            _html = html;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri != _expectedUri)
                throw new InvalidOperationException($"Unexpected request URL: {request.RequestUri}");

            var response = new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(_html)
            };
            return Task.FromResult(response);
        }
    }

    [Theory]
    [InlineData("15.-20.04.2026", "2026-04-15")]
    [InlineData("18.04.-04.05.2026", "2026-04-18")]
    [InlineData("01.05.2026", "2026-05-01")]
    public void NormalizeDuvCalendarDateToYyyyMmDd_ParsesKnownDuvDateFormats(string input, string expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.NormalizeDuvCalendarDateToYyyyMmDd(input));
    }

    // ── NormalizeDateToYyyyMmDd ────────────────────────────────────────────────

    [Theory]
    [InlineData("20250914", "2025-09-14")]
    [InlineData("2025-09-14", "2025-09-14")]
    [InlineData("2025-01-01", "2025-01-01")]
    [InlineData("20231231", "2023-12-31")]
    [InlineData("5 september 2026 kl. 10:00", "2026-09-05")]
    [InlineData("söndag 5 september 2026 kl. 10:00", "2026-09-05")]
    [InlineData("5 september 2026 kl. 10:00 start", "2026-09-05")]
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
    [InlineData("randotrail", "trail")]
    [InlineData("RandoTrail", "trail")]
    [InlineData("Trail running", "trail")]
    [InlineData("marathon", null)]
    [InlineData("Trail, marathon", "trail")]
    [InlineData("Stiløp", "trail")]
    [InlineData("stig", "trail")]
    [InlineData("Terreng", "cross country")]
    [InlineData("terräng", "cross country")]
    [InlineData("terrain", "cross country")]
    [InlineData("Asfalt", "road")]
    [InlineData("landsväg", "road")]
    [InlineData("gateløp", "road")]
    [InlineData("grus", "gravel")]
    [InlineData("stafett", "relay")]
    [InlineData("motbakke", "uphill")]
    [InlineData("vertical", "uphill")]
    [InlineData("vertikal", "uphill")]
    [InlineData("trappeløp", "stairs")]
    [InlineData("trappor", "stairs")]
    [InlineData("hinderløp", "obstacle course")]
    [InlineData("OCR", "obstacle course")]
    [InlineData("Stiløp/Terreng", "trail, cross country")]
    [InlineData("Stiløp_Terreng", "trail, cross country")]
    [InlineData(null, null)]
    [InlineData("", null)]
    public void NormalizeRaceType_ConvertsRaceTypesCorrectly(string? input, string? expected)
    {
        Assert.Equal(expected, RaceScrapeDiscovery.NormalizeRaceType(input));
    }

    [Fact]
    public void NormalizeRaceType_DeduplicatesTokens()
    {
        var result = RaceScrapeDiscovery.NormalizeRaceType("trail, Trail, stig");
        Assert.Equal("trail", result);
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
    public void ParseTraceDeTrailCalendarEvents_EmitsOneJobPerTraceWithBothUrls()
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
                },
                {
                    "evtID": "7562",
                    "itraEvtID": "16686",
                    "orgaID": null,
                    "nom": "Ultra Trail Gazelles Sahara 5.0",
                    "logo": "LogoEvent7562_7444.jpg",
                    "img": "ImgEvent7562_7444.jpg",
                    "label": "ultra-trail-gazelles-sahara-5-0-2026",
                    "localite": "Hazoua",
                    "country": "TN",
                    "depcode": "TN",
                    "dateDeb": "2026-01-10",
                    "dateFin": "2026-01-10",
                    "tags": "",
                    "distances": "110.39_69.16_36.69",
                    "sports": "randotrail_randotrail_randotrail",
                    "traceIDs": "300613_300614_300615"
                }
              ]
            }
            """;

        var jobs = RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(payload);

        // One job per event (not per trace ID).
        Assert.Equal(3, jobs.Count);

        var ultra4m = Assert.Single(jobs, j => j.Name == "Ultra Tour 4 Massifs");
        Assert.Equal(2, ultra4m.TraceDeTrailItraUrls!.Count);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/12345", ultra4m.TraceDeTrailItraUrls[0].AbsoluteUri);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/67890", ultra4m.TraceDeTrailItraUrls[1].AbsoluteUri);
        Assert.Equal("https://tracedetrail.fr/en/event/ultra-tour-4-massifs", ultra4m.TraceDeTrailEventUrl!.AbsoluteUri);
        Assert.Equal("50 km, 100 km", ultra4m.Distance);
        Assert.Equal("FR", ultra4m.Country);
        Assert.Equal("https://tracedetrail.fr/events/race.jpg", ultra4m.ImageUrl);

        var another = Assert.Single(jobs, j => j.Name == "Another Race");
        Assert.Single(another.TraceDeTrailItraUrls!);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/11111", another.TraceDeTrailItraUrls[0].AbsoluteUri);
        Assert.Equal("https://tracedetrail.fr/en/event/another-race", another.TraceDeTrailEventUrl!.AbsoluteUri);
        Assert.Equal("42 km", another.Distance);
        Assert.Equal("trail", another.RaceType);
        // logo fallback when img is null
        Assert.Equal("https://tracedetrail.fr/events/logo.jpg", another.ImageUrl);
        Assert.Equal("https://tracedetrail.fr/events/logo.jpg", another.LogoUrl);

        var sahara = Assert.Single(jobs, j => j.Name == "Ultra Trail Gazelles Sahara 5.0");
        Assert.Equal(3, sahara.TraceDeTrailItraUrls!.Count);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/300613", sahara.TraceDeTrailItraUrls[0].AbsoluteUri);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/300614", sahara.TraceDeTrailItraUrls[1].AbsoluteUri);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/300615", sahara.TraceDeTrailItraUrls[2].AbsoluteUri);
        Assert.Equal("https://tracedetrail.fr/en/event/ultra-trail-gazelles-sahara-5-0-2026", sahara.TraceDeTrailEventUrl!.AbsoluteUri);
        Assert.Equal(new Dictionary<string, string> { ["tracedetrailEventId"] = "7562", ["itraEventId"] = "16686" }, sahara.ExternalIds);
        Assert.Equal("Ultra Trail Gazelles Sahara 5.0", sahara.Name);
        Assert.Equal("https://tracedetrail.fr/events/ImgEvent7562_7444.jpg", sahara.ImageUrl);
        Assert.Equal("https://tracedetrail.fr/events/LogoEvent7562_7444.jpg", sahara.LogoUrl);
        Assert.Equal("trail", sahara.RaceType);
        Assert.Equal("2026-01-10", sahara.Date);
        Assert.Equal("110.4 km, 69.2 km, 36.7 km", sahara.Distance);
        Assert.Equal("TN", sahara.Country);
    }

    [Fact]
    public void ParseTraceDeTrailCalendarEvents_ReturnsEmptyForBlankInput()
    {
        Assert.Empty(RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(""));
        Assert.Empty(RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents("{}"));
    }

    [Fact]
    public void ParseTraceDeTrailCalendarEvents_HandlesSlugWithSlashes()
    {
        var payload = @"{""success"":1,""data"": [{""label"": ""foo/bar/baz.com"", ""traceIDs"": ""123""}]}";
        var jobs = RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(payload);
        var job = Assert.Single(jobs);
        Assert.Null(job.TraceDeTrailEventUrl);
        Assert.Equal("https://baz.com/", job.WebsiteUrl!.AbsoluteUri);

        // Also test a plain domain
        payload = @"{""success"":1,""data"": [{""label"": ""www.sormlands100.com"", ""traceIDs"": ""123""}]}";
        jobs = RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(payload);
        job = Assert.Single(jobs);
        Assert.Null(job.TraceDeTrailEventUrl);
        Assert.Equal("https://www.sormlands100.com/", job.WebsiteUrl!.AbsoluteUri);

        // And a normal tracedetrail slug
        payload = @"{""success"":1,""data"": [{""label"": ""ultra-tour-4-massifs"", ""traceIDs"": ""123""}]}";
        jobs = RaceScrapeDiscovery.ParseTraceDeTrailCalendarEvents(payload);
        job = Assert.Single(jobs);
        Assert.Equal("https://tracedetrail.fr/en/event/ultra-tour-4-massifs", job.TraceDeTrailEventUrl!.AbsoluteUri);
        Assert.Null(job.WebsiteUrl);
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
    [Fact]
    public void DeriveOrganizerKey_RunsignupSlugIdsAreNormalized()
    {
        Assert.Equal("runsignup.com~Race~TX~Longview~LongviewTrailRunsSpring",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://runsignup.com/Race/TX/Longview/LongviewTrailRunsSpring")));

        Assert.Equal("runsignup.com~HABANERO",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://runsignup.com/HABANERO")));

        Assert.Equal("runsignup.com~Race~UT~ParkCity~TripleTrail",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://runsignup.com/Race/Events/UT/ParkCity/TripleTrail")));
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

}
