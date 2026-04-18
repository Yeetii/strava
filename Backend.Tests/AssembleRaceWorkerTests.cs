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

        // BFS found 4 routes, all without coordinates → 4 point races.
        Assert.Equal(4, races.Count);
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

        // Should be sorted: 8 km, 12 km, 33 km, 45 km
        var distances = races
            .Select(r => r.Properties.TryGetValue("distance", out var d) ? d?.ToString() : null)
            .ToList();

        Assert.Equal("8 km", distances[0]);
        Assert.Equal("12 km", distances[1]);
        Assert.Equal("33 km", distances[2]);
        Assert.Equal("45 km", distances[3]);
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

        // The 45 km route in BFS has date = 2026-08-23.
        var races = AssembleRaceWorker.AssembleRaces(doc);

        var race45 = races.Single(r => r.Properties.TryGetValue("distance", out var d) && d?.ToString() == "45 km");
        // Discovery date is 2026-09-05, bfs route date is 2026-08-23.
        // 2026-09-05 > 2026-08-23, so discovery date should win.
        Assert.Equal("2026-09-05", race45.Properties["date"].ToString());
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

        var race45 = races.Single(r => r.Properties.TryGetValue("distance", out var d) && d?.ToString() == "45 km");
        // BFS date 2026-08-23 > discovery date 2025-09-05 → bfs date wins.
        Assert.Equal("2026-08-23", race45.Properties["date"].ToString());
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
                            Name = "Gbgtrailrun 45 km",
                            Distance = "45 km",
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
