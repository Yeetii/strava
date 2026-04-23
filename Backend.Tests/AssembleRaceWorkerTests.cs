using Backend;
using Shared.Models;

namespace Backend.Tests;

public class AssembleRaceWorkerTests
{
    // ── AssembleRaces — no scraper output ────────────────────────────────

    [Fact]
    public void AssembleRaces_NoScrapers_ReturnsPointFromDiscovery()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "gbgtrailrun.se",
            Url = "https://gbgtrailrun.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:00:00Z",
                        Name = "Merrell Göteborg Trailrun",
                        Date = "2026-09-05",
                        Latitude = 57.738877,
                        Longitude = 12.0403897,
                        Distance = "50 km, 33 km",
                        Country = "SE",
                    }
                ]
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Single(races);
        Assert.Equal("race:gbgtrailrun.se-0", races[0].Id);
        Assert.Equal("gbgtrailrun.se-0", races[0].FeatureId);
        Assert.Equal("race", races[0].Kind);
        Assert.Equal("Point", races[0].Geometry.Type.ToString());
        Assert.Equal("Merrell Göteborg Trailrun", races[0].Properties["name"].ToString());
        Assert.Equal("2026-09-05", races[0].Properties["date"].ToString());
    }

    [Fact]
    public void AssembleRaces_NoScrapers_DeduplicatesSameDistanceDiscoveryByLaterDate()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "ludvikaffi.se",
            Url = "https://ludvikaffi.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-20T16:44:05.4039290Z",
                        Name = "Ludvika stadslopp",
                        Date = "2026-04-23",
                        Latitude = 60.1506247,
                        Longitude = 15.1812487,
                        Distance = "10 km, 5 km",
                        Country = "SE",
                        Location = "Charlie Normans torg, Ludvika, Sweden",
                        SourceUrls = ["http://ludvikaffi.se/"]
                    },
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-20T16:44:05.4039290Z",
                        Name = "Ludvika stadslopp",
                        Date = "2025-05-17",
                        Latitude = 60.1506247,
                        Longitude = 15.1812487,
                        Distance = "10 km, 5 km",
                        Country = "SE",
                        Location = "Charlie Normans torg, Ludvika, Sweden",
                        SourceUrls = ["http://ludvikaffi.se/"]
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-20T16:48:50.5634799Z",
                    ImageUrl = "https://klubbenonline.objects.dc-sto1.glesys.net/q2jYvI69EltmQVRV/YXgAUQEW2tZIo1l07VFTSwh2mh4cSs9OryNq2UZy.jpg",
                    LogoUrl = "https://ludvikaffi.klubbenonline.se/build/assets/favicon-6b968d32.ico",
                    ExtractedName = "Ludvika FFI",
                    ExtractedDate = "2026-05-23",
                    StartFee = "3000",
                    Currency = "SEK"
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Single(races);
        Assert.Equal("2026-04-23", races[0].Properties["date"].ToString());
        Assert.Equal("Ludvika stadslopp", races[0].Properties["name"].ToString());
    }

    [Fact]
    public void DistancesRoughMatchKm_Ultra509And511_IsMatch()
    {
        Assert.True(AssembleRaceWorker.DistancesRoughMatchKm(509, 511));
        Assert.True(AssembleRaceWorker.DistancesRoughMatchKm(511, 509));
    }

    [Fact]
    public void DistancesRoughMatchKm_Marathon45And50_IsNotMatch()
    {
        Assert.False(AssembleRaceWorker.DistancesRoughMatchKm(45, 50));
    }

    [Fact]
    public void BuildCanonicalRouteKmMap_RejectsTransitiveNeighborChainWhenMinAndMaxDifferTooMuch()
    {
        // Consecutive pairs match (~2%), but 100 vs 104 exceeds 3% of 104 → must not be one cluster.
        Assert.True(AssembleRaceWorker.DistancesRoughMatchKm(100, 102));
        Assert.True(AssembleRaceWorker.DistancesRoughMatchKm(102, 104));
        Assert.False(AssembleRaceWorker.DistancesRoughMatchKm(100, 104));

        var map = AssembleRaceWorker.BuildCanonicalRouteKmMap([100.0, 102.0, 104.0]);
        Assert.Equal(100.0, map[100.0]);
        Assert.Equal(100.0, map[102.0]);
        Assert.Equal(104.0, map[104.0]);
    }

    [Fact]
    public void BuildCanonicalRouteKmMap_UltraCluster_EveryPairWithin3PercentOfLonger()
    {
        var kms = new List<double> { 509.0, 510.0, 511.0 };
        foreach (var i in Enumerable.Range(0, kms.Count))
            foreach (var j in Enumerable.Range(i + 1, kms.Count - i - 1))
                Assert.True(AssembleRaceWorker.DistancesRoughMatchKm(kms[i], kms[j]));

        var map = AssembleRaceWorker.BuildCanonicalRouteKmMap(kms);
        Assert.All(kms, k => Assert.Equal(509.0, map[k]));
    }

    [Fact]
    public void AssembleRaces_NoScrapers_DeduplicatesExGotland509And511SameName()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "exgotland.se",
            Url = "https://exgotland.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-20T16:44:05.4039290Z",
                        Name = "Ex Gotland Run",
                        Date = "2026-06-01",
                        Latitude = 57.6348,
                        Longitude = 18.2948,
                        Distance = "509 km",
                        Country = "SE",
                    },
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-21T10:00:00Z",
                        Name = "Ex Gotland Run",
                        Date = "2026-06-01",
                        Latitude = 57.6348,
                        Longitude = 18.2948,
                        Distance = "511 km",
                        Country = "SE",
                    }
                ]
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Single(races);
        Assert.Equal("Ex Gotland Run", races[0].Properties["name"].ToString());
    }

    [Fact]
    public void AssembleRaces_NoScrapers_DoesNotDeduplicateSameDistanceDifferentName()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "example.se",
            Url = "https://example.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-20T16:44:05.4039290Z",
                        Name = "Race One",
                        Date = "2026-04-23",
                        Latitude = 60.1506247,
                        Longitude = 15.1812487,
                        Distance = "10 km",
                    },
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-20T16:44:05.4039290Z",
                        Name = "Race Two",
                        Date = "2026-04-23",
                        Latitude = 60.1506247,
                        Longitude = 15.1812487,
                        Distance = "10 km",
                    }
                ]
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Equal(2, races.Count);
    }

    [Fact]
    public void AssembleRaces_NoScrapersNoCoords_ReturnsEmpty()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "example.se",
            Url = "https://example.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:00:00Z",
                        Name = "Some Race",
                        // No coordinates
                    }
                ]
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Empty(races);
    }

    // ── AssembleRaces — with BFS scraper output ──────────────────────────

    [Fact]
    public void AssembleRaces_BfsRoutes_CreatesPointRacesFromDiscoveryCoords()
    {
        var doc = MakeGbgTrailrunDoc();

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // BFS found 4 routes + 1 unclaimed discovery distance (21 km) → 5 point races.
        Assert.Equal(5, races.Count);
        // Stable IDs based on organizer key + sorted index.
        Assert.All(races, r => Assert.StartsWith("race:gbgtrailrun.se-", r.Id));
        // All should be Points (no route coordinates, but discovery has lat/lng).
        Assert.All(races, r => Assert.Equal("Point", r.Geometry.Type.ToString()));
    }

    [Fact]
    public void AssembleRaces_BfsRoutes_SortedByDistanceKm()
    {
        var doc = MakeGbgTrailrunDoc();

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // Scraper routes sorted: 8 km, 12 km, 33 km, 49 km; then unclaimed 21 km appended.
        var distances = races
            .Select(r => r.Properties.TryGetValue("distance", out var d) ? d?.ToString() : null)
            .ToList();

        Assert.Equal("8 km", distances[0]);
        Assert.Equal("12 km", distances[1]);
        Assert.Equal("33 km", distances[2]);
        Assert.Equal("49 km", distances[3]);
        Assert.Equal("21 km", distances[4]);
    }

    [Fact]
    public void AssembleRaces_BfsRoutes_MergesDiscoveryMetadata()
    {
        var doc = MakeGbgTrailrunDoc();

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // Discovery (loppkartan) has country=SE, location=Kvibergs Park
        Assert.All(races, r =>
        {
            Assert.Equal("SE", r.Properties["country"].ToString());
            Assert.Equal("Kvibergs Park, Gothenburg, Sweden", r.Properties["location"].ToString());
        });
    }

    [Fact]
    public void AssembleRaces_BfsRouteHasDate_UsesRouteDate()
    {
        var doc = MakeGbgTrailrunDoc();

        // The 49 km route in BFS has date = 2026-08-23.
        var races = AssembleRaceWorker.AssembleRaces(doc);

        var race49 = races.Single(r => r.Properties.TryGetValue("distance", out var d) && d?.ToString() == "49 km");
        // Discovery date is 2026-09-05, bfs route date is 2026-08-23.
        // 2026-09-05 > 2026-08-23, so discovery date should win.
        Assert.Equal("2026-09-05", race49.Properties["date"].ToString());
    }

    [Fact]
    public void AssembleRaces_BfsRouteHasNewerDate_UsesBfsDate()
    {
        var doc = MakeGbgTrailrunDoc();

        // Override discovery date with older date so bfs date is newer.
        doc.Discovery!["loppkartan"][0] = new SourceDiscovery
        {
            DiscoveredAtUtc = "2026-04-18T21:00:00Z",
            Name = "Merrell Göteborg Trailrun",
            Date = "2025-09-05",  // older than bfs route date 2026-08-23
            Latitude = 57.738877,
            Longitude = 12.0403897,
            Distance = "50 km, 33 km, 21 km, 12 km, 8 km",
            Country = "SE",
            Location = "Kvibergs Park, Gothenburg, Sweden",
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        var race49 = races.Single(r => r.Properties.TryGetValue("distance", out var d) && d?.ToString() == "49 km");
        // BFS date 2026-08-23 > discovery date 2025-09-05 → bfs date wins.
        Assert.Equal("2026-08-23", race49.Properties["date"].ToString());
    }

    [Fact]
    public void AssembleRaces_RouteWithCoordinates_BuildsLineString()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "trailrace.se",
            Url = "https://trailrace.se/",
            Discovery = null,
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:00:00Z",
                    WebsiteUrl = "https://trailrace.se/",
                    Routes =
                    [
                        new ScrapedRouteOutput
                        {
                            Name = "Trail 10 km",
                            Distance = "10 km",
                            Coordinates =
                            [
                                [18.0686, 59.3293],
                                [18.0700, 59.3300],
                                [18.0750, 59.3350],
                            ],
                        }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Single(races);
        Assert.Equal("LineString", races[0].Geometry.Type.ToString());
        Assert.Equal("race:trailrace.se-0", races[0].Id);
    }

    [Fact]
    public void AssembleRaces_WebsiteUrlSetOnAllRaces()
    {
        var doc = MakeGbgTrailrunDoc();

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // BFS scraper has no websiteUrl, so falls back to doc.Url.
        Assert.All(races, r =>
            Assert.Equal("https://gbgtrailrun.se/", r.Properties["website"].ToString()));
    }

    // ── MergeDiscovery ────────────────────────────────────────────────────

    [Fact]
    public void MergeDiscovery_UtmbWinsOverLoppkartan()
    {
        var discovery = new Dictionary<string, List<SourceDiscovery>>
        {
            ["utmb"] =
            [
                new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "UTMB Race", Country = "FR", RaceType = "trail" }
            ],
            ["loppkartan"] =
            [
                new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Loppkartan Race", Country = "SE" }
            ]
        };

        var merged = AssembleRaceWorker.MergeDiscovery(discovery);

        Assert.Equal("UTMB Race", merged.Name);
        Assert.Equal("FR", merged.Country);
        Assert.Equal("trail", merged.RaceType);
    }

    [Fact]
    public void MergeDiscovery_FillsFromLowerPriorityWhenHigherIsNull()
    {
        var discovery = new Dictionary<string, List<SourceDiscovery>>
        {
            ["utmb"] =
            [
                new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "UTMB Race" }
                // No location in UTMB entry
            ],
            ["loppkartan"] =
            [
                new SourceDiscovery { DiscoveredAtUtc = "Z", Location = "Kvibergs Park" }
            ]
        };

        var merged = AssembleRaceWorker.MergeDiscovery(discovery);

        Assert.Equal("UTMB Race", merged.Name);
        Assert.Equal("Kvibergs Park", merged.Location);
    }

    [Fact]
    public void MergeDiscovery_EmptyReturnsDefaults()
    {
        var merged = AssembleRaceWorker.MergeDiscovery(null);

        Assert.Null(merged.Name);
        Assert.Null(merged.Date);
    }

    // ── CollectRoutes ─────────────────────────────────────────────────────

    [Fact]
    public void CollectRoutes_PutsCoordRoutesFirst()
    {
        var scrapers = new Dictionary<string, ScraperOutput>
        {
            ["bfs"] = new ScraperOutput
            {
                ScrapedAtUtc = "Z",
                Routes =
                [
                    new ScrapedRouteOutput { Name = "No Coords", Distance = "10 km" },
                    new ScrapedRouteOutput
                    {
                        Name = "Has Coords", Distance = "20 km",
                        Coordinates = [[18.0, 59.0], [18.1, 59.1]]
                    }
                ]
            }
        };

        var routes = AssembleRaceWorker.CollectRoutes(scrapers);

        Assert.Equal(2, routes.Count);
        Assert.Equal("Has Coords", routes[0].Route.Name);  // coords first
        Assert.Equal("No Coords", routes[1].Route.Name);
    }

    [Fact]
    public void CollectRoutes_RespectsScraperPriority()
    {
        var scrapers = new Dictionary<string, ScraperOutput>
        {
            ["bfs"] = new ScraperOutput
            {
                ScrapedAtUtc = "Z",
                Routes = [new ScrapedRouteOutput
                {
                    Name = "BFS Route",
                    Distance = "10 km",
                    Coordinates = [[18.0, 59.0], [18.1, 59.1]]
                }]
            },
            ["utmb"] = new ScraperOutput
            {
                ScrapedAtUtc = "Z",
                Routes = [new ScrapedRouteOutput
                {
                    Name = "UTMB Route",
                    Distance = "10 km",
                    Coordinates = [[17.0, 58.0], [17.1, 58.1]]
                }]
            }
        };

        var routes = AssembleRaceWorker.CollectRoutes(scrapers);

        Assert.Equal(2, routes.Count);
        Assert.Equal("UTMB Route", routes[0].Route.Name);  // utmb > bfs
    }

    // ── PickBestDate ─────────────────────────────────────────────────────

    [Theory]
    [InlineData("bfs", "2026-09-05", "2025-01-01", "2026-09-05")]   // bfs newer wins
    [InlineData("bfs", "2025-01-01", "2026-09-05", "2026-09-05")]   // discovery newer wins
    [InlineData("bfs", null, "2026-09-05", "2026-09-05")]            // no route date → discovery
    [InlineData("bfs", "2026-09-05", null, "2026-09-05")]            // no discovery date → route
    [InlineData("utmb", "2026-09-05", "2025-01-01", "2025-01-01")]  // non-bfs route date ignored
    [InlineData(null, "2026-09-05", "2025-01-01", "2025-01-01")]     // null scraper → discovery
    public void PickBestDate_ReturnsExpected(
        string? scraperKey, string? routeDate, string? discoveryDate, string? expected)
    {
        var result = AssembleRaceWorker.PickBestDate(scraperKey, routeDate, discoveryDate);
        Assert.Equal(expected, result);
    }

    // ── ParseDistanceKm ─────────────────────────────────────────────────

    [Theory]
    [InlineData("33 km", 33.0)]
    [InlineData("8 km", 8.0)]
    [InlineData("50 km, 33 km", 50.0)]    // first token only
    [InlineData("5k", 5.0)]
    [InlineData(null, null)]
    [InlineData("", null)]
    [InlineData("marathon", null)]         // unparseable
    public void ParseDistanceKm_ReturnsExpected(string? input, double? expected)
    {
        var result = AssembleRaceWorker.ParseDistanceKm(input);
        Assert.Equal(expected, result);
    }

    // ── Discovery / scraper merging ─────────────────────────────────────

    [Fact]
    public void AssembleRaces_DiscoverySingleDistance_PrefersDiscoveryPropsExceptDate()
    {
        // Discovery has a single matching distance entry with rich metadata.
        // Scraper (bfs) has a route at the same distance with its own name/elevation/etc.
        // Expected: discovery name, elevation, country, etc. win; scraper date wins (newer).
        var doc = new RaceOrganizerDocument
        {
            Id = "singlerace.se",
            Url = "https://singlerace.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:00:00Z",
                        Name = "Official Race Name",
                        Date = "2025-06-01",
                        Latitude = 59.0,
                        Longitude = 18.0,
                        Distance = "33 km",
                        ElevationGain = 1200,
                        Country = "SE",
                        Location = "Stockholm",
                        RaceType = "trail",
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:00:00Z",
                    Routes =
                    [
                        new ScrapedRouteOutput
                        {
                            Name = "Scraped Route Name 33 km",
                            Distance = "33 km",
                            ElevationGain = 900,
                            Date = "2026-06-01",
                        }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Single(races);
        var props = races[0].Properties;
        // Discovery props should win.
        Assert.Equal("Official Race Name", props["name"].ToString());
        Assert.Equal(1200.0, (double)props["elevationGain"]);
        Assert.Equal("SE", props["country"].ToString());
        Assert.Equal("Stockholm", props["location"].ToString());
        Assert.Equal("trail", props["raceType"].ToString());
        // Newer date from bfs should win.
        Assert.Equal("2026-06-01", props["date"].ToString());
    }

    [Fact]
    public void AssembleRaces_DiscoveryMultiDistance_PrefersDiscoveryPropsForMatchingRoutes()
    {
        // Discovery lists multiple distances. Scraper has two routes that match
        // two of the discovery distances. Discovery props should still win.
        var doc = new RaceOrganizerDocument
        {
            Id = "multirace.se",
            Url = "https://multirace.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:00:00Z",
                        Name = "Grand Trail Event",
                        Date = "2025-09-15",
                        Latitude = 59.0,
                        Longitude = 18.0,
                        Distance = "50 km, 21 km",
                        Country = "SE",
                        Location = "Gothenburg",
                        ElevationGain = 2000,
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:00:00Z",
                    Routes =
                    [
                        new ScrapedRouteOutput
                        {
                            Name = "BFS 21k",
                            Distance = "21 km",
                            ElevationGain = 500,
                            Date = "2026-09-15",
                        },
                        new ScrapedRouteOutput
                        {
                            Name = "BFS 50k",
                            Distance = "50 km",
                            ElevationGain = 1500,
                            Date = "2026-09-15",
                        }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Equal(2, races.Count);
        // Both routes should use the discovery name, country, location.
        Assert.All(races, r =>
        {
            Assert.Equal("Grand Trail Event", r.Properties["name"].ToString());
            Assert.Equal("SE", r.Properties["country"].ToString());
            Assert.Equal("Gothenburg", r.Properties["location"].ToString());
        });
        // Per-route distance should still come from the scraper route (more specific).
        var distances = races.Select(r => r.Properties["distance"].ToString()).OrderBy(d => d).ToList();
        Assert.Contains("21 km", distances);
        Assert.Contains("50 km", distances);
        // Newer dates from bfs should win.
        Assert.All(races, r => Assert.Equal("2026-09-15", r.Properties["date"].ToString()));
    }

    [Fact]
    public void AssembleRaces_DiscoveryMultiDistance_IncludesItraMetadata()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "multirace.se",
            Url = "https://multirace.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:00:00Z",
                        Name = "Grand Trail Event",
                        Date = "2025-09-15",
                        Latitude = 59.0,
                        Longitude = 18.0,
                        Distance = "50 km, 21 km",
                        Country = "SE",
                        Location = "Gothenburg",
                        ElevationGain = 2000,
                        ItraPoints = 5,
                        ItraNationalLeague = true,
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:00:00Z",
                    Routes =
                    [
                        new ScrapedRouteOutput
                        {
                            Name = "BFS 21k",
                            Distance = "21 km",
                            ElevationGain = 500,
                            Date = "2026-09-15",
                        },
                        new ScrapedRouteOutput
                        {
                            Name = "BFS 50k",
                            Distance = "50 km",
                            ElevationGain = 1500,
                            Date = "2026-09-15",
                        }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Equal(2, races.Count);
        Assert.All(races, r =>
        {
            Assert.Equal(5.0, (double)r.Properties[RaceScrapeDiscovery.PropItraPoints]);
            Assert.True((bool)r.Properties[RaceScrapeDiscovery.PropItraNationalLeague]);
        });
    }

    [Fact]
    public void AssembleRaces_DuplicateGpxSameDistance_PrefersNewestRouteDateAndOnlyCreatesDiscoveryMatches()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "duplicategpx.se",
            Url = "https://duplicategpx.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-19T22:00:00Z",
                        Name = "Long Race",
                        Date = "2027-05-15",
                        Latitude = 53.0,
                        Longitude = 8.0,
                        Distance = "160.9 km",
                        Country = "DE",
                    },
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-19T22:00:00Z",
                        Name = "Medium Race",
                        Date = "2027-05-15",
                        Latitude = 53.1,
                        Longitude = 9.0,
                        Distance = "101.8 km",
                        Country = "DE",
                    },
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-19T22:00:00Z",
                        Name = "Short Race",
                        Date = "2027-05-15",
                        Latitude = 53.2,
                        Longitude = 9.5,
                        Distance = "51.3 km",
                        Country = "DE",
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-19T23:00:00Z",
                    WebsiteUrl = "https://duplicategpx.se/",
                    Routes =
                    [
                        new ScrapedRouteOutput
                        {
                            Name = "Medium Race GPX 1",
                            Distance = "101.8 km",
                            Date = "2027-05-20",
                        },
                        new ScrapedRouteOutput
                        {
                            Name = "Medium Race GPX 2",
                            Distance = "101.8 km",
                            Date = "2027-06-01",
                        },
                        new ScrapedRouteOutput
                        {
                            Name = "Short Race GPX",
                            Distance = "51.3 km",
                            Date = "2027-05-16",
                        },
                        new ScrapedRouteOutput
                        {
                            Name = "Long Race GPX",
                            Distance = "160.9 km",
                            Date = "2027-05-14",
                        }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        Assert.Equal(3, races.Count);
        Assert.Single(races.Where(r => r.Properties["distance"].ToString() == "101.8 km"));
        Assert.Single(races.Where(r => r.Properties["distance"].ToString() == "51.3 km"));
        Assert.Single(races.Where(r => r.Properties["distance"].ToString() == "160.9 km"));

        var mediumRace = races.Single(r => r.Properties["distance"].ToString() == "101.8 km");
        Assert.Equal("2027-06-01", mediumRace.Properties["date"].ToString());
        Assert.Equal("Medium Race", mediumRace.Properties["name"].ToString());
    }

    // ── FindBestDiscoveryForRoute — specificity matching ────────────────

    [Fact]
    public void FindBestDiscovery_PrefersSingleDistanceOverMulti()
    {
        // Single-distance entry at 33 km (loppkartan) vs multi-distance from utmb.
        // Even though utmb has higher source priority, the single-distance entry is more specific.
        var flat = new List<(string Source, SourceDiscovery Entry)>
        {
            ("utmb", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "UTMB Multi", Distance = "50 km, 33 km" }),
            ("loppkartan", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Exact 33k", Distance = "33 km" }),
        };
        var fallback = new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Fallback" };

        var result = AssembleRaceWorker.FindBestDiscoveryForRoute(33.0, flat, fallback);

        Assert.Equal("Exact 33k", result.Name);
    }

    [Fact]
    public void FindBestDiscovery_SourcePriorityWithinSameSpecificity()
    {
        // Two single-distance entries at 33 km; utmb should win.
        var flat = new List<(string Source, SourceDiscovery Entry)>
        {
            ("utmb", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "UTMB 33k", Distance = "33 km" }),
            ("loppkartan", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Lopp 33k", Distance = "33 km" }),
        };
        var fallback = new SourceDiscovery { DiscoveredAtUtc = "Z" };

        var result = AssembleRaceWorker.FindBestDiscoveryForRoute(33.0, flat, fallback);

        Assert.Equal("UTMB 33k", result.Name);
    }

    [Fact]
    public void FindBestDiscovery_FallsBackWhenNoMatch()
    {
        var flat = new List<(string Source, SourceDiscovery Entry)>
        {
            ("loppkartan", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Lopp", Distance = "50 km" }),
        };
        var fallback = new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Fallback" };

        var result = AssembleRaceWorker.FindBestDiscoveryForRoute(10.0, flat, fallback);

        Assert.Equal("Fallback", result.Name);
    }

    [Fact]
    public void FindBestDiscovery_ClosestDeltaWinsWithinTolerance()
    {
        // Route at 100 km. Discoveries at 101 km and 101.5 km both within tolerance.
        // 101 km is closer → should win, even though 101.5 comes from a higher-priority source.
        var flat = new List<(string Source, SourceDiscovery Entry)>
        {
            ("utmb", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "UTMB 101.5k", Distance = "101.5 km" }),
            ("loppkartan", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Lopp 101k", Distance = "101 km" }),
        };
        var fallback = new SourceDiscovery { DiscoveredAtUtc = "Z" };

        var result = AssembleRaceWorker.FindBestDiscoveryForRoute(100.0, flat, fallback);

        Assert.Equal("Lopp 101k", result.Name);
    }

    [Fact]
    public void FindBestDiscovery_SameDeltaUsesSourcePriority()
    {
        // Route at 100 km. Two discoveries both at 101 km (same delta).
        // utmb comes first in the list → should win.
        var flat = new List<(string Source, SourceDiscovery Entry)>
        {
            ("utmb", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "UTMB 101k", Distance = "101 km" }),
            ("loppkartan", new SourceDiscovery { DiscoveredAtUtc = "Z", Name = "Lopp 101k", Distance = "101 km" }),
        };
        var fallback = new SourceDiscovery { DiscoveredAtUtc = "Z" };

        var result = AssembleRaceWorker.FindBestDiscoveryForRoute(100.0, flat, fallback);

        Assert.Equal("UTMB 101k", result.Name);
    }

    // ── Unclaimed discovery distances ────────────────────────────────────

    [Fact]
    public void AssembleRaces_UnclaimedDiscoveryDistances_CreatePointRaces()
    {
        // Discovery: 50 km, 33 km, 21 km. Scraper: only 33 km.
        // 50 km and 21 km are unclaimed → should appear as additional point races.
        var doc = new RaceOrganizerDocument
        {
            Id = "multi.se",
            Url = "https://multi.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:00:00Z",
                        Name = "Multi Trail",
                        Date = "2026-09-01",
                        Latitude = 59.0,
                        Longitude = 18.0,
                        Distance = "50 km, 33 km, 21 km",
                        Country = "SE",
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:00:00Z",
                    Routes =
                    [
                        new ScrapedRouteOutput { Name = "BFS 33k", Distance = "33 km" }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // 1 scraper route + 2 unclaimed discovery distances = 3 races
        Assert.Equal(3, races.Count);
        var distances = races.Select(r => r.Properties["distance"].ToString()).OrderBy(d => d).ToList();
        Assert.Contains("21 km", distances);
        Assert.Contains("33 km", distances);
        Assert.Contains("50 km", distances);
        // All should be points (no route coordinates).
        Assert.All(races, r => Assert.Equal("Point", r.Geometry.Type.ToString()));
    }

    [Fact]
    public void AssembleRaces_CloseDistanceClaimsDiscovery()
    {
        // Scraper route at 49 km should claim the 50 km discovery distance (under 3% of 50 km).
        var doc = new RaceOrganizerDocument
        {
            Id = "close.se",
            Url = "https://close.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:00:00Z",
                        Name = "Close Match",
                        Latitude = 59.0,
                        Longitude = 18.0,
                        Distance = "50 km",
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:00:00Z",
                    Routes =
                    [
                        new ScrapedRouteOutput { Name = "BFS 49k", Distance = "49 km" }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // 49 km route claims the 50 km discovery → no unclaimed distances.
        Assert.Single(races);
        Assert.Equal("49 km", races[0].Properties["distance"].ToString());
    }

    [Fact]
    public void AssembleRaces_SpecificDiscoveryUsedPerRoute()
    {
        // Two discovery sources: utmb with per-distance entries and loppkartan with multi-distance.
        // Routes should prefer the utmb single-distance entry for matching properties.
        var doc = new RaceOrganizerDocument
        {
            Id = "specific.se",
            Url = "https://specific.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["utmb"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "Z", Name = "UTMB Ultra 100",
                        Distance = "100 km", ElevationGain = 6000, Country = "FR",
                        Latitude = 45.0, Longitude = 6.0,
                    },
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "Z", Name = "UTMB CCC",
                        Distance = "101 km", ElevationGain = 6100, Country = "FR",
                        Latitude = 45.0, Longitude = 6.0,
                    }
                ],
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "Z", Name = "Generic Event",
                        Distance = "100 km, 101 km, 55 km",
                        Country = "SE",
                        Latitude = 45.0, Longitude = 6.0,
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "Z",
                    Routes =
                    [
                        new ScrapedRouteOutput { Distance = "100 km" },
                        new ScrapedRouteOutput { Distance = "101 km" },
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // Route at 100km → UTMB Ultra 100 (single-distance entry wins over loppkartan multi)
        var race100 = races.Single(r => r.Properties["distance"].ToString() == "100 km");
        Assert.Equal("UTMB Ultra 100", race100.Properties["name"].ToString());
        Assert.Equal(6000.0, (double)race100.Properties["elevationGain"]);

        // Route at 101km → UTMB CCC (single-distance entry)
        var race101 = races.Single(r => r.Properties["distance"].ToString() == "101 km");
        Assert.Equal("UTMB CCC", race101.Properties["name"].ToString());
        Assert.Equal(6100.0, (double)race101.Properties["elevationGain"]);

        // 55km from loppkartan is unclaimed → should get a point race
        var race55 = races.SingleOrDefault(r => r.Properties["distance"].ToString() == "55 km");
        Assert.NotNull(race55);
        Assert.Equal("Point", race55.Geometry.Type.ToString());
    }

    [Fact]
    public void AssembleRaces_NonNumericDistanceLabel_CreatesRaceWithDiscoveryName()
    {
        // Discovery 1: "Lydinge Resort Stafett" with "42 km, 21 km, Stafett"
        // Discovery 2: "HOKA Helsingborg Half Marathon" with "21 km"
        // Scraper: one route with "21 km, 42 km"
        //
        // Expected: 3 races:
        //   21 km → scraped route, matched to "HOKA Helsingborg Half Marathon" (single-distance wins)
        //   42 km → scraped route, matched to "Lydinge Resort Stafett" (has 42 km)
        //   Stafett → point race using "Lydinge Resort Stafett" name (non-numeric unclaimed label)
        var doc = new RaceOrganizerDocument
        {
            Id = "helsingborgmarathon.se",
            Url = "https://helsingborgmarathon.se/stafett/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:02:35Z",
                        Name = "Lydinge Resort Stafett",
                        Date = "2026-09-05",
                        Latitude = 56.0475746,
                        Longitude = 12.6902033,
                        Distance = "42 km, 21 km, Stafett",
                        Country = "SE",
                        Location = "Sundstorget, Helsingborg, Sweden",
                        RaceType = "relay",
                        County = "Skåne",
                    },
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:02:35Z",
                        Name = "HOKA Helsingborg Half Marathon",
                        Date = "2025-09-06",
                        Latitude = 56.0478422,
                        Longitude = 12.6890694,
                        Distance = "21 km",
                        Country = "SE",
                        Location = "Dunkers Kulturhus, Helsingborg, Sweden",
                        RaceType = "road",
                        County = "Skåne",
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:00:00Z",
                    WebsiteUrl = "https://helsingborgmarathon.se/halvmaraton/",
                    ImageUrl = "https://helsingborgmarathon.se/img/popup-arrow.png",
                    LogoUrl = "https://helsingborgmarathon.se/wp-content/uploads/2021/06/Top-Symbol.png",
                    Routes =
                    [
                        new ScrapedRouteOutput
                        {
                            SourceUrl = "https://helsingborgmarathon.se/halvmaraton/",
                            Name = "Banan HOKA Half 25",
                            Distance = "21 km",
                            ElevationGain = 190,
                            StartFee = "800",
                            Currency = "SEK",
                        }
                    ]
                }
            }
        };

        var races = AssembleRaceWorker.AssembleRaces(doc);

        // 1 scraped route (21 km) + 2 unclaimed: 42 km (numeric) + Stafett (label) = 3
        Assert.Equal(3, races.Count);

        // 21 km: scraped route, best discovery = "HOKA Helsingborg Half Marathon" (single-distance)
        var race21 = races.Single(r => r.Properties["distance"].ToString() == "21 km");
        Assert.Equal("HOKA Helsingborg Half Marathon", race21.Properties["name"].ToString());

        // 42 km: unclaimed numeric, best discovery = "Lydinge Resort Stafett" (has 42 km)
        var race42 = races.Single(r => r.Properties["distance"].ToString() == "42 km");
        Assert.Equal("Lydinge Resort Stafett", race42.Properties["name"].ToString());
        Assert.Equal("Point", race42.Geometry.Type.ToString());

        // Stafett: unclaimed non-numeric label, discovery = "Lydinge Resort Stafett"
        var raceStafett = races.Single(r => r.Properties["distance"].ToString() == "Stafett");
        Assert.Equal("Lydinge Resort Stafett", raceStafett.Properties["name"].ToString());
        Assert.Equal("Point", raceStafett.Geometry.Type.ToString());
    }

    // ── ParseDistanceList ───────────────────────────────────────────────

    [Theory]
    [InlineData("50 km, 33 km, 21 km", new[] { 50.0, 33.0, 21.0 })]
    [InlineData("8 km", new[] { 8.0 })]
    [InlineData("", new double[0])]
    [InlineData(null, new double[0])]
    public void ParseDistanceList_ReturnsExpected(string? input, double[] expected)
    {
        var result = AssembleRaceWorker.ParseDistanceList(input);
        Assert.Equal(expected, result);
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static RaceOrganizerDocument MakeGbgTrailrunDoc() =>
        new()
        {
            Id = "gbgtrailrun.se",
            Url = "https://gbgtrailrun.se/",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["loppkartan"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-18T21:02:35Z",
                        Name = "Merrell Göteborg Trailrun",
                        Date = "2026-09-05",
                        Latitude = 57.738877,
                        Longitude = 12.0403897,
                        Distance = "50 km, 33 km, 21 km, 12 km, 8 km",
                        Country = "SE",
                        Location = "Kvibergs Park, Gothenburg, Sweden",
                        RaceType = "trail",
                    }
                ]
            },
            Scrapers = new Dictionary<string, ScraperOutput>
            {
                ["bfs"] = new ScraperOutput
                {
                    ScrapedAtUtc = "2026-04-18T22:14:22Z",
                    ImageUrl = "https://media.gbgtrailrun.se/2024/08/8D6A6376-kopia-1024x683.jpg",
                    LogoUrl = "https://media.gbgtrailrun.se/2020/02/Namnlos.png",
                    ExtractedName = "Gbgtrailrun",
                    Routes =
                    [
                        new ScrapedRouteOutput
                        {
                            SourceUrl = "https://gbgtrailrun.se/8km/",
                            Name = "Gbgtrailrun 8 km",
                            Distance = "8 km",
                            ElevationGain = 250,
                            ImageUrl = "https://media.gbgtrailrun.se/2020/09/IMG_3513-1-1024x640.jpg",
                            LogoUrl = "https://media.gbgtrailrun.se/2020/02/Namnlos.png",
                        },
                        new ScrapedRouteOutput
                        {
                            SourceUrl = "https://gbgtrailrun.se/12-km/",
                            Name = "Gbgtrailrun 12 km",
                            Distance = "12 km",
                            ElevationGain = 350,
                            ImageUrl = "https://media.gbgtrailrun.se/2024/08/8D6A6411-1024x683.jpg",
                            LogoUrl = "https://media.gbgtrailrun.se/2020/02/Namnlos.png",
                        },
                        new ScrapedRouteOutput
                        {
                            SourceUrl = "https://gbgtrailrun.se/33km/",
                            Name = "Gbgtrailrun 33 km",
                            Distance = "33 km",
                            ElevationGain = 800,
                            ImageUrl = "https://media.gbgtrailrun.se/2022/10/Gbg-Trail_Internet_028-1024x684.jpg",
                            LogoUrl = "https://media.gbgtrailrun.se/2020/02/Namnlos.png",
                        },
                        new ScrapedRouteOutput
                        {
                            SourceUrl = "https://gbgtrailrun.se/tavlingsinfo/",
                            Name = "Gbgtrailrun 49 km",
                            Distance = "49 km",
                            Date = "2026-08-23",
                            StartFee = "200",
                            Currency = "SEK",
                            ImageUrl = "https://media.gbgtrailrun.se/2022/09/IMG_9984-1024x683.jpg",
                            LogoUrl = "https://media.gbgtrailrun.se/2020/02/Namnlos.png",
                        },
                    ]
                }
            }
        };
}
