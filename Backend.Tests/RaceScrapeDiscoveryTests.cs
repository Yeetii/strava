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
    public void ParseTrailrunningSwedenCalendarPage_ExtractsEventsFromHtml()
    {
        const string html = """
            <div id="event_48578_0" class="eventon_list_event evo_eventtop scheduled  event no_et event_48578_0" data-event_id="48578" data-ri="0r" data-time="1777629600-1777679400" data-colr="#192a4a" itemscope itemtype="http://schema.org/Event">
                <div class="evo_event_schema" style="display:none">
                    <a itemprop="url" href="https://trailrunningsweden.se/events/pop-up-run-akulla-bokskogar-2/"></a>
                    <meta itemprop="image" content="https://imgix-trs.trailrunningsweden.se/wp-content/uploads/2022/06/Osbeck_bokskog_600x400.jpg" />
                    <meta itemprop="startDate" content="2026-05-01T10:00+02:00" />
                    <meta itemprop="endDate" content="2026-05-01T18:30+02:00" />
                </div>
                <a data-gmap_status="null" data-exlk="0" style="border-color: #192a4a;" id="evc_177762960048578" class="desc_trig featured_event 1 sin_val evcal_list_a" data-ux_val="3">
                    <span class="evoet_c2 evoet_cx ">
                        <span class="evoet_dayblock evcal_cblock " data-bgcolor="#192a4a" data-smon="maj" data-syr="2026" data-bub="">
                            <span class="evo_start "><em class="date">01</em><em class="month">maj</em><em class="time">10:00</em></span>
                        </span>
                    </span>
                    <span class="evoet_c3 evoet_cx evcal_desc">
                        <span class="evoet_title evcal_desc2 evcal_event_title" itemprop="name">Pop up Run Åkulla bokskogar</span>
                        <span class="event_location_attrs" data-location_address="Rolfstorp, Varberg" data-location_type="lonlat" data-location_name="Öströö Fårfarm" data-location_url="https://trailrunningsweden.se/event-location/ostroo-farfarm/" data-location_status="true" data-latlng="57.1463512,12.4558229"></span>
                        <span class="evoet_subtitle evo_below_title"><span class="evcal_event_subtitle">15-17 km</span></span>
                    </span>
                </a>
            </div>
            """;

        var jobs = RaceScrapeDiscovery.ParseTrailrunningSwedenCalendarPage(html);

        var job = Assert.Single(jobs);
        Assert.Equal("Pop up Run Åkulla bokskogar", job.Name);
        Assert.Equal("2026-05-01", job.Date);
        Assert.Equal("15-17 km", job.Distance);
        Assert.Equal("SE", job.Country);
        Assert.Equal("Öströö Fårfarm, Rolfstorp, Varberg", job.Location);
        Assert.Equal(57.1463512, job.Latitude);
        Assert.Equal(12.4558229, job.Longitude);
        Assert.Equal("https://trailrunningsweden.se/events/pop-up-run-akulla-bokskogar-2/", job.WebsiteUrl!.AbsoluteUri);
        Assert.Equal("trail", job.RaceType);
        Assert.Equal("https://imgix-trs.trailrunningsweden.se/wp-content/uploads/2022/06/Osbeck_bokskog_600x400.jpg", job.ImageUrl);
        Assert.Equal(new Dictionary<string, string> { ["trailrunningsweden"] = "48578" }, job.ExternalIds);
    }

    [Fact]
    public void ParseLopplistanTrailPage_ExtractsValidEvents()
    {
        const string html = """
            <div class="race " style="border-left-color: #219653">
              <div class="race__section">
                <div class="race__date">
                  <div class="race__date__content">
                    <time datetime="2026-05-01">
                      1 Maj
                    </time>
                  </div>
                </div>
                <div class="race__vertical">
                  <div class="race__organization">
                    <a class="race__link" href=/lopp/jarv-aventyr-ramsvik/>
                      Järv Äventyr Ramsvik
                    </a>
                  </div>
                  <div class="race__distance">
                    10.0, 5.0 km
                  </div>
                  <div class="race__location">
                    Ramsvik
                  </div>
                </div>
              </div>
              <div class="race__section">
                <div class="race__activity" title=Trail>
                  <span class="hidden-sm-down">Trail</span> 🌲
                </div>
              </div>
            </div>
            """;

        var jobs = RaceScrapeDiscovery.ParseLopplistanTrailPage(html, new Uri("https://lopplistan.se/sverige/trail/"));

        var job = Assert.Single(jobs);
        Assert.Equal("Järv Äventyr Ramsvik", job.Name);
        Assert.Equal("2026-05-01", job.Date);
        Assert.Equal("10.0, 5.0 km", job.Distance);
        Assert.Equal("Ramsvik", job.Location);
        Assert.Equal("SE", job.Country);
        Assert.Equal("trail", job.RaceType);
        Assert.Equal("https://lopplistan.se/lopp/jarv-aventyr-ramsvik/", job.WebsiteUrl!.AbsoluteUri);
    }

    [Fact]
    public void ExtractTrailrunningSwedenEventWebsiteUrl_FindsHemsidaLink()
    {
        const string html = """
            <a class="evcal_evdata_row evo_clik_row " href="https://trailrunningsweden.se/lopargrupper/pop-up-runs/" target="_blank">
                <span class="evcal_evdata_icons"><i class="fa fa-link"></i></span>
                <h3 class="evo_h3">Hemsida</h3>
            </a>
            """;

        var websiteUrl = RaceScrapeDiscovery.ExtractTrailrunningSwedenEventWebsiteUrl(html, new Uri("https://trailrunningsweden.se/events/pop-up-run-akulla-bokskogar-2/"));

        Assert.Equal("https://trailrunningsweden.se/lopargrupper/pop-up-runs/", websiteUrl!.AbsoluteUri);
    }

    [Fact]
    public void ExtractLopplistanEventWebsiteUrl_FindsTillLoppetLink()
    {
        const string html = """
            <a href="https://example.com" class="button">Till loppet!</a>
            """;

        var websiteUrl = RaceScrapeDiscovery.ExtractLopplistanEventWebsiteUrl(html, new Uri("https://lopplistan.se/lopp/bararydsloppet/"));

        Assert.Equal("https://example.com/", websiteUrl!.AbsoluteUri);
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
    public void TrailrunningSwedenEventPage_IsIncludedInSourceUrls()
    {
        var job = new ScrapeJob(
            WebsiteUrl: new Uri("https://trailrunningsweden.se/lopargrupper/pop-up-runs/"),
            TrailrunningSwedenEventUrl: new Uri("https://trailrunningsweden.se/events/pop-up-run-akulla-bokskogar-2/"));

        var discovery = job.ToSourceDiscovery();

        Assert.Equal(new[]
        {
            "https://trailrunningsweden.se/lopargrupper/pop-up-runs/",
            "https://trailrunningsweden.se/events/pop-up-run-akulla-bokskogar-2/"
        }, discovery.SourceUrls);
    }

    [Fact]
    public void ScrapeJob_ToSourceDiscovery_DeduplicatesAndPreservesSourceUrlOrder()
    {
        var job = new ScrapeJob(
            UtmbUrl: new Uri("https://utmb.world/races/100k"),
            TraceDeTrailItraUrls: new[]
            {
                new Uri("https://tracedetrail.fr/trace/getTraceItra/1"),
                new Uri("https://tracedetrail.fr/trace/getTraceItra/1")
            },
            TraceDeTrailEventUrl: new Uri("https://tracedetrail.fr/en/event/x"),
            ItraEventPageUrl: new Uri("https://itra.run/Races/RaceDetails/10"),
            RunagainUrl: new Uri("https://runagain.com/find-event/x"),
            WebsiteUrl: new Uri("https://example.com/"),
            BetrailUrl: new Uri("https://www.betrail.run/race/x/2025"));

        var discovery = job.ToSourceDiscovery();

        Assert.Equal(new[]
        {
            "https://utmb.world/races/100k",
            "https://tracedetrail.fr/trace/getTraceItra/1",
            "https://tracedetrail.fr/en/event/x",
            "https://itra.run/Races/RaceDetails/10",
            "https://runagain.com/find-event/x",
            "https://example.com/",
            "https://www.betrail.run/race/x/2025"
        }, discovery.SourceUrls);
    }

    [Fact]
    public void DeriveEventKeyFromJob_FallsBackToNameAndDistanceWhenNoUrl()
    {
        var job = new ScrapeJob(Name: "Eco Trail Race", Distance: "50 km");

        var result = RaceScrapeDiscovery.DeriveEventKeyFromJob(job);

        Assert.NotNull(result);
        Assert.Equal("eco-trail-race-50-km", result?.EventKey);
        Assert.Equal("name://eco-trail-race-50-km", result?.CanonicalUrl);
    }

    [Fact]
    public void DeriveEventKeyFromJob_ChoosesBestAvailableSourceUrl()
    {
        var job = new ScrapeJob(
            WebsiteUrl: new Uri("https://example.com/event"),
            UtmbUrl: new Uri("https://utmb.world/races/100k"));

        var result = RaceScrapeDiscovery.DeriveEventKeyFromJob(job);

        Assert.NotNull(result);
        Assert.Equal("example.com~event", result?.EventKey);
        Assert.Equal("https://example.com/event", result?.CanonicalUrl);
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
        var anotherUrls = another.TraceDeTrailItraUrls!;
        Assert.NotNull(anotherUrls);
        Assert.Single(anotherUrls);
        Assert.Equal("https://tracedetrail.fr/trace/getTraceItra/11111", anotherUrls[0].AbsoluteUri);
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

    [Fact]
    public void ParseBeTrailEvents_EmitsOneJobPerRaceWithRaceLevelData()
    {
        // Realistic-shaped payload mirroring https://www.betrail.run/api/events-drizzle:
        // { body: { events: [ { ..., trail: { website, alias2, place, country }, races: [...] } ] } }
        const string payload = """
            {
              "body": {
                "events": [
                  {
                    "id": 13485985,
                    "title": "Epicurienne Trail - 2025",
                    "alias": "2025",
                    "date": 1757714400,
                    "country": "FR",
                    "trail": {
                      "id": 960170,
                      "alias2": "epicurienne-trail",
                      "website": "https://www.epicurienne-trail.com/trail-ultra-gourmand",
                      "place": "Castelnau d'Estrétefonds",
                      "country": "FR",
                      "organizer": "CRBP Raid 31"
                    },
                    "races": [
                      {
                        "id": 13485986,
                        "alias": "16km",
                        "race_name": "La Gastronome",
                        "title": "Epicurienne Trail - 2025 - 16km | La Gastronome",
                        "date": 1757714400,
                        "distance": 16,
                        "elevation": 350,
                        "category": "nature",
                        "race_type": "solo",
                        "official_ranking_url": "https://resultat.chrono-start.fr/race/4468/ranking?export=pdf"
                      },
                      {
                        "id": 13485987,
                        "alias": "22km",
                        "race_name": "L'Epicurienne",
                        "title": "Epicurienne Trail - 2025 - 22km | L'Epicurienne",
                        "date": 1757714400,
                        "distance": 22,
                        "elevation": 550,
                        "category": "nature_xl",
                        "race_type": "solo"
                      }
                    ]
                  }
                ]
              }
            }
            """;

        var jobs = RaceScrapeDiscovery.ParseBeTrailEvents(payload).ToArray();

        Assert.Equal(2, jobs.Length);

        // Per-race jobs share the canonical event URL (BeTrail has no per-race URLs).
        var first = jobs[0];
        Assert.Equal("https://www.betrail.run/race/epicurienne-trail/2025", first.BetrailUrl!.AbsoluteUri);
        Assert.Equal("https://www.epicurienne-trail.com/trail-ultra-gourmand", first.WebsiteUrl!.AbsoluteUri);
        Assert.Equal("16 km", first.Distance);
        Assert.Equal(350, first.ElevationGain);
        Assert.Equal("FR", first.Country);
        Assert.Equal("Castelnau d'Estrétefonds", first.Location);
        Assert.Equal("CRBP Raid 31", first.Organizer);
        Assert.Equal("2025-09-12", first.Date); // unix 1757714400 → 2025-09-12 UTC
        Assert.Equal("13485986", first.ExternalIds!["betrail"]);

        var second = jobs[1];
        Assert.Equal("https://www.betrail.run/race/epicurienne-trail/2025", second.BetrailUrl!.AbsoluteUri);
        Assert.Equal("22 km", second.Distance);
        Assert.Equal(550, second.ElevationGain);
        Assert.Equal("13485987", second.ExternalIds!["betrail"]);
    }

    [Fact]
    public void ParseBeTrailEvents_FallsBackToEventJobWhenNoRaces()
    {
        const string payload = """
            {
              "body": {
                "events": [
                  {
                    "id": 999,
                    "title": "Solo Event - 2026",
                    "alias": "2026",
                    "date": 1776988800,
                    "country": "BE",
                    "trail": {
                      "alias2": "solo-event",
                      "website": "https://www.solo-event.be",
                      "place": "Brussels",
                      "country": "BE"
                    }
                  }
                ]
              }
            }
            """;

        var jobs = RaceScrapeDiscovery.ParseBeTrailEvents(payload).ToArray();

        var job = Assert.Single(jobs);
        Assert.Equal("Solo Event - 2026", job.Name);
        Assert.Equal("https://www.betrail.run/race/solo-event/2026", job.BetrailUrl!.AbsoluteUri);
        Assert.Equal("https://www.solo-event.be/", job.WebsiteUrl!.AbsoluteUri);
        Assert.Equal("BE", job.Country);
        Assert.Equal("trail", job.RaceType);
    }

    [Fact]
    public void ParseBeTrailEvents_IgnoresRankingExportsAsWebsiteUrl()
    {
        // trail.website is unavailable; only the ranking/PDF export is set — we must NOT pick it
        // up as the race website (it's a results page, not the race site).
        const string payload = """
            {
              "body": {
                "events": [
                  {
                    "id": 1,
                    "title": "X - 2026",
                    "alias": "2026",
                    "date": 1776988800,
                    "country": "BE",
                    "trail": {
                      "alias2": "x",
                      "website": "https://resultat.chrono-start.fr/race/4468/ranking?export=pdf",
                      "country": "BE"
                    },
                    "races": [
                      {
                        "id": 2,
                        "alias": "10km",
                        "race_name": "Short",
                        "distance": 10,
                        "elevation": 100,
                        "category": "trail",
                        "race_type": "solo"
                      }
                    ]
                  }
                ]
              }
            }
            """;

        var jobs = RaceScrapeDiscovery.ParseBeTrailEvents(payload).ToArray();

        var job = Assert.Single(jobs);
        Assert.Null(job.WebsiteUrl);
        Assert.Equal("https://www.betrail.run/race/x/2026", job.BetrailUrl!.AbsoluteUri);
    }

    [Fact]
    public void ParseRunagainSearchResults_ParsesArrayFieldsGpsAndExternalIds()
    {
        const string payload = """
            {
              "hits": [
                {
                  "post_title": "RunAgain Trail",
                  "post_url": "trail-event",
                  "country": "US",
                  "place": "Boulder",
                  "county": "Boulder County",
                  "date": "2026-09-12",
                  "gps": [40.014986, -105.270546],
                  "length": [10, 21],
                  "race_type": ["Trail"],
                  "terrain_type": ["Asfalt"],
                  "cover_image": "https://example.com/image.jpg",
                  "race_guid": "runagain-123"
                }
              ]
            }
            """;

        var jobs = RaceScrapeDiscovery.ParseRunagainSearchResults(payload).ToArray();

        var job = Assert.Single(jobs);
        Assert.Equal("https://runagain.com/find-event/trail-event", job.RunagainUrl!.AbsoluteUri);
        Assert.Equal("RunAgain Trail", job.Name);
        Assert.Equal("US", job.Country);
        Assert.Equal("Boulder", job.Location);
        Assert.Equal("Boulder County", job.County);
        Assert.Equal("2026-09-12", job.Date);
        Assert.Equal("10 km, 21 km", job.Distance);
        Assert.Equal("trail, road", job.RaceType);
        Assert.Equal("Trail", job.TypeLocal);
        Assert.Equal(40.014986, job.Latitude);
        Assert.Equal(-105.270546, job.Longitude);
        Assert.Equal("https://example.com/image.jpg", job.ImageUrl);
        Assert.Equal(new Dictionary<string, string> { ["runagain"] = "runagain-123" }, job.ExternalIds);
    }

    [Fact]
    public void TryParseMarathonKeyword_ReturnsExpectedKilometers()
    {
        Assert.True(RaceScrapeDiscovery.TryParseMarathonKeyword("MARATHON", out var marathonKm));
        Assert.Equal(42, marathonKm);

        Assert.True(RaceScrapeDiscovery.TryParseMarathonKeyword("Half marathon", out var halfMarathonKm));
        Assert.Equal(21, halfMarathonKm);

        Assert.False(RaceScrapeDiscovery.TryParseMarathonKeyword("Not a race", out _));
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

        // UltraSignup: the ?did= query param uniquely identifies the event, and every
        // .aspx tab (register / results / entrants) for the same did collapses to one key.
        Assert.Equal("ultrasignup.com~register.aspx?did=104195",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://ultrasignup.com/register.aspx?did=104195")));
        Assert.Equal("ultrasignup.com~register.aspx?did=119001",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://ultrasignup.com/register.aspx?did=119001")));
        Assert.Equal("ultrasignup.com~register.aspx?did=119001",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://ultrasignup.com/results_event.aspx?did=119001")));
        Assert.Equal("ultrasignup.com~register.aspx?did=119001",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://ultrasignup.com/entrants_event.aspx?did=119001")));
        // No `did` query — fall back to the raw first path segment.
        Assert.Equal("ultrasignup.com~register.aspx",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://ultrasignup.com/register.aspx")));

        Assert.Equal("my.raceresult.com~376823",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://my.raceresult.com/376823/info")));

        Assert.Equal("raceroster.com~events~2026~112366~day-of-the-dead-day-1-and-day-2",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://raceroster.com/events/2026/112366/day-of-the-dead-day-1-and-day-2/page/location-and-course-details")));

        Assert.Equal("welcu.com~utcb",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://welcu.com/utcb/utcb2025")));

        Assert.Equal("facebook.com~profile.php?id=61552551957764",
            RaceOrganizerClient.DeriveOrganizerKey(new Uri("https://www.facebook.com/profile.php?id=61552551957764")));
    }

    [Theory]
    // BeTrail: strip year/tab after the race slug.
    [InlineData("https://www.betrail.run/race/dalmacija.utra.trail/2025/overview", "betrail.run~race~dalmacija.utra.trail")]
    [InlineData("https://www.betrail.run/race/dalmacija.utra.trail", "betrail.run~race~dalmacija.utra.trail")]
    [InlineData("https://www.betrail.run/race/epicurienne-trail/2025", "betrail.run~race~epicurienne-trail")]
    [InlineData("https://www.betrail.run/en/race/epicurienne-trail/2025", "betrail.run~race~epicurienne-trail")]
    // ITRA: strip year + id after the race name.
    [InlineData("https://itra.run/Races/RaceDetails/Boo.Trail.Run.2025.Boo.Trail.Half.Marathon/2025/103052", "itra.run~Races~RaceDetails~Boo.Trail.Run.2025.Boo.Trail.Half.Marathon")]
    [InlineData("https://itra.run/Races/RaceDetails/Some.Event", "itra.run~Races~RaceDetails~Some.Event")]
    // Google Sites — classic /site/<name> flavour.
    [InlineData("https://sites.google.com/site/gaxultra/the-gax-walk%C3%A6rle-noctum", "sites.google.com~site~gaxultra")]
    [InlineData("https://sites.google.com/site/gaxultra", "sites.google.com~site~gaxultra")]
    // Google Sites — workspace/custom-domain flavour.
    [InlineData("https://sites.google.com/oni-p.org/oni-p/", "sites.google.com~oni-p.org")]
    [InlineData("https://sites.google.com/oni-p.org", "sites.google.com~oni-p.org")]
    // Google Sites — new /view/<name> flavour.
    [InlineData("https://sites.google.com/view/fall-back-blast", "sites.google.com~view~fall-back-blast")]
    [InlineData("https://sites.google.com/view/fall-back-blast/races", "sites.google.com~view~fall-back-blast")]
    // Klikego: strip discipline + registration-id segments and any query string.
    [InlineData("https://www.klikego.com/inscription/les-foulees-dacigne-acigne-au-feminin-2025/running-course-a-pied/1356384566470-11?tab=-1", "klikego.com~inscription~les-foulees-dacigne-acigne-au-feminin-2025")]
    [InlineData("https://www.klikego.com/inscription/courses-nature-de-parigne-2025/course-a-pied-running/1395738014939-12", "klikego.com~inscription~courses-nature-de-parigne-2025")]
    [InlineData("https://www.klikego.com/inscription/trail-des-croquants-2025/", "klikego.com~inscription~trail-des-croquants-2025")]
    public void DeriveOrganizerKey_NewPlatformsAreNormalized(string url, string expected)
    {
        Assert.Equal(expected, RaceOrganizerClient.DeriveOrganizerKey(new Uri(url)));
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
