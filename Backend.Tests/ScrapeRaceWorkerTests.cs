using Azure.Messaging.ServiceBus;
using Shared.Models;
using Shared.Services;

namespace Backend.Tests;

public class ScrapeRaceWorkerTests
{
    [Fact]
    public void DeserializeMessage_AcceptsJsonAutomaticMessage()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"organizerKey":"sample.com","isUrgent":false}"""));

        var message = ScrapeRaceWorker.DeserializeMessage(received);

        Assert.Equal("sample.com", message.OrganizerKey);
        Assert.False(message.IsUrgent);
    }

    [Fact]
    public void DeserializeMessage_FallsBackToPlainTextUrgentMessage()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("sample.com"));

        var message = ScrapeRaceWorker.DeserializeMessage(received);

        Assert.Equal("sample.com", message.OrganizerKey);
        Assert.True(message.IsUrgent);
    }

    [Fact]
    public void IsFreshEnoughForAutomaticScrape_ReturnsTrueForRecentScrape()
    {
        var now = new DateTime(2026, 4, 29, 12, 0, 0, DateTimeKind.Utc);
        var doc = new RaceOrganizerDocument
        {
            Id = "sample.com",
            Url = "https://sample.com",
            LastScrapedUtc = now.AddDays(-2).ToString("o")
        };

        Assert.True(ScrapeRaceWorker.IsFreshEnoughForAutomaticScrape(doc, now));
    }

    [Fact]
    public void ComputeHashes_IgnoresScrapedAtUtcButDetectsPropertyAndRouteChanges()
    {
        var baseline = new ScraperOutput
        {
            ScrapedAtUtc = "2026-04-29T10:00:00Z",
            ExtractedName = "Race",
            Routes =
            [
                new ScrapedRouteOutput
                {
                    Coordinates = [[1, 2], [3, 4]],
                    Name = "10 km",
                    SourceUrl = "https://sample.com/10"
                }
            ]
        };

        var rescraped = new ScraperOutput
        {
            ScrapedAtUtc = "2026-04-29T11:00:00Z",
            ExtractedName = "Race",
            Routes =
            [
                new ScrapedRouteOutput
                {
                    Coordinates = [[1, 2], [3, 4]],
                    Name = "10 km",
                    SourceUrl = "https://sample.com/10"
                }
            ]
        };

        var propertyChange = new ScraperOutput
        {
            ScrapedAtUtc = "2026-04-29T11:00:00Z",
            ExtractedName = "Race Updated",
            Routes = rescraped.Routes
        };

        var routeChange = new ScraperOutput
        {
            ScrapedAtUtc = "2026-04-29T11:00:00Z",
            ExtractedName = "Race",
            Routes =
            [
                new ScrapedRouteOutput
                {
                    Coordinates = [[1, 2], [5, 6]],
                    Name = "10 km",
                    SourceUrl = "https://sample.com/10"
                }
            ]
        };

        var baselineHashes = ScrapeRaceWorker.ComputeHashes(baseline);
        var rescrapedHashes = ScrapeRaceWorker.ComputeHashes(rescraped);
        var propertyHashes = ScrapeRaceWorker.ComputeHashes(propertyChange);
        var routeHashes = ScrapeRaceWorker.ComputeHashes(routeChange);

        Assert.Equal(baselineHashes.PropertiesHash, rescrapedHashes.PropertiesHash);
        Assert.Equal(baselineHashes.RoutesHash, rescrapedHashes.RoutesHash);
        Assert.NotEqual(baselineHashes.PropertiesHash, propertyHashes.PropertiesHash);
        Assert.Equal(baselineHashes.RoutesHash, propertyHashes.RoutesHash);
        Assert.Equal(baselineHashes.PropertiesHash, routeHashes.PropertiesHash);
        Assert.NotEqual(baselineHashes.RoutesHash, routeHashes.RoutesHash);
    }

    [Fact]
    public void CollectBfsUrls_RestrictsScopedHostsToOrganizerScope()
    {
        var doc = new RaceOrganizerDocument
        {
            Id = "strava.com~route",
            Url = "https://www.strava.com/route/12345/example",
            Discovery = new Dictionary<string, List<SourceDiscovery>>
            {
                ["manual"] =
                [
                    new SourceDiscovery
                    {
                        DiscoveredAtUtc = "2026-04-29T00:00:00Z",
                        SourceUrls =
                        [
                            "https://www.strava.com/route/99999/other",
                            "https://www.strava.com/athletes/12345",
                            "https://www.strava.com/route/12345/example"
                        ]
                    }
                ]
            }
        };

        var urls = ScrapeRaceWorker.CollectBfsUrls(doc);

        Assert.Contains(urls, u => u.AbsoluteUri == "https://www.strava.com/route/12345/example");
        Assert.Contains(urls, u => u.AbsoluteUri == "https://www.strava.com/route/99999/other");
        Assert.DoesNotContain(urls, u => u.AbsoluteUri == "https://www.strava.com/athletes/12345");
    }

    [Theory]
    [InlineData("https://www.strava.com/route/12342/somesite", "strava.com~route")]
    [InlineData("https://airtable.com/shrS5egjcGtRHpEGy", "airtable.com~shrS5egjcGtRHpEGy")]
    public void DeriveOrganizerKey_ScopesNewSluggableHosts(string url, string expected)
    {
        Assert.Equal(expected, RaceOrganizerClient.DeriveOrganizerKey(new Uri(url)));
    }

    [Fact]
    public void CanBfsCrawlUri_OnlyAllowsScopedPagesForSluggableHost()
    {
        Assert.True(OrganizerUrlRules.CanBfsCrawlUri(new Uri("https://www.strava.com/route/12342/somesite"), "strava.com~route"));
        Assert.False(OrganizerUrlRules.CanBfsCrawlUri(new Uri("https://www.strava.com/user/erik"), "strava.com~route"));
        Assert.False(OrganizerUrlRules.CanBfsCrawlUri(new Uri("https://www.strava.com/"), "strava.com~route"));
    }
}