using Stigar.Core.Services;
using Stigar.Core.Jobs;
using Stigar.Workers;

namespace Stigar.Workers.Tests;

public class RaceDiscoveryMessageTests
{
    [Fact]
    public void DiscoveryJob_SerializesTypedPayload()
    {
        var message = new RaceDiscoveryMessage("duv", 3);

        var json = System.Text.Json.JsonSerializer.Serialize(message, new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web));

        Assert.Contains("\"agent\":\"duv\"", json, StringComparison.Ordinal);
        Assert.Contains("\"page\":3", json, StringComparison.Ordinal);
    }

    [Fact]
    public void DiscoveryJob_SerializesPaginationMetadata()
    {
        var message = new RaceDiscoveryMessage("itra", 2, Country: "AF", CountryIndex: 0, CountryPage: 2, CountryPageCount: 3, RacesOnPage: 50);

        var json = System.Text.Json.JsonSerializer.Serialize(message, new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web));

        Assert.Contains("\"country\":\"AF\"", json, StringComparison.Ordinal);
        Assert.Contains("\"racesOnPage\":50", json, StringComparison.Ordinal);
        Assert.Equal("discovery:itra:2", RaceDiscoveryService.BuildDiscoveryJobId(message));
    }

    [Fact]
    public void DiscoveryJobId_IsStableAcrossRetriesForSamePage()
    {
        var first = new RaceDiscoveryMessage("itra", 7, Country: "US", CountryIndex: 226, CountryPage: 3, CountryPageCount: 4, RacesOnPage: 40);
        var second = new RaceDiscoveryMessage("itra", 7, Country: "US", CountryIndex: 226, CountryPage: 3, CountryPageCount: 4, RacesOnPage: 40);

        Assert.Equal(RaceDiscoveryService.BuildDiscoveryJobId(first), RaceDiscoveryService.BuildDiscoveryJobId(second));
        Assert.Equal("discovery:itra:7", RaceDiscoveryService.BuildDiscoveryJobId(first));
    }

    [Fact]
    public void DiscoveryJobId_DiffersForDifferentPages()
    {
        var first = new RaceDiscoveryMessage("itra", 7);
        var second = new RaceDiscoveryMessage("itra", 8);

        Assert.NotEqual(RaceDiscoveryService.BuildDiscoveryJobId(first), RaceDiscoveryService.BuildDiscoveryJobId(second));
    }

    [Fact]
    public void DeserializeMessage_DefaultsMissingPageToFirstPage()
    {
        var received = """{"agent":"tracedetrail"}""";

        var message = RaceDiscoveryWorker.DeserializeMessage(received);

        Assert.NotNull(message);
        Assert.Equal("tracedetrail", message.Agent);
        Assert.Null(message.Page);
        Assert.Equal(1, message.CurrentPage);
    }

    [Fact]
    public void DeserializeMessage_AcceptsCamelCasePage()
    {
        var received = """{"agent":"duv","page":4}""";

        var message = RaceDiscoveryWorker.DeserializeMessage(received);

        Assert.NotNull(message);
        Assert.Equal("duv", message.Agent);
        Assert.Equal(4, message.Page);
        Assert.Equal(4, message.CurrentPage);
    }

    [Fact]
    public void DeserializeMessage_AcceptsItraMetadata()
    {
        var received = """{"agent":"itra","page":7,"country":"US","countryIndex":226,"countryPage":3,"countryPageCount":4,"racesOnPage":40}""";

        var message = RaceDiscoveryWorker.DeserializeMessage(received);

        Assert.NotNull(message);
        Assert.Equal("US", message.Country);
        Assert.Equal(226, message.CountryIndex);
        Assert.Equal(3, message.CountryPage);
        Assert.Equal(4, message.CountryPageCount);
        Assert.Equal(40, message.RacesOnPage);
    }

    [Fact]
    public void TraceDeTrailPage_CoversOneMonthAtATime()
    {
        var firstPage = DiscoverTraceDeTrailRaces.MonthForPage(1);
        var secondPage = DiscoverTraceDeTrailRaces.MonthForPage(2);

        Assert.Equal(firstPage.AddMonths(1).Year, secondPage.Year);
        Assert.Equal(firstPage.AddMonths(1).Month, secondPage.Month);
    }

    [Fact]
    public void ItraCountryPageCount_SplitsLargeCountriesIntoFiftyRacePages()
    {
        Assert.Equal(1, DiscoverItraRaces.GetCountryPageCount(0));
        Assert.Equal(1, DiscoverItraRaces.GetCountryPageCount(50));
        Assert.Equal(3, DiscoverItraRaces.GetCountryPageCount(140));
        Assert.Equal(40, DiscoverItraRaces.CountRacesOnPage(140, 3));
    }

    [Fact]
    public void ItraSliceCountryJobs_UsesFiftyRaceBoundaries()
    {
        var jobs = Enumerable.Range(1, 140)
            .Select(index => new ScrapeJob(Name: $"Race {index}"))
            .ToArray();

        var firstPage = DiscoverItraRaces.SliceCountryJobs(jobs, 1);
        var secondPage = DiscoverItraRaces.SliceCountryJobs(jobs, 2);
        var thirdPage = DiscoverItraRaces.SliceCountryJobs(jobs, 3);

        Assert.Equal(50, firstPage.Count);
        Assert.Equal("Race 1", firstPage[0].Name);
        Assert.Equal("Race 50", firstPage[^1].Name);

        Assert.Equal(50, secondPage.Count);
        Assert.Equal("Race 51", secondPage[0].Name);
        Assert.Equal("Race 100", secondPage[^1].Name);

        Assert.Equal(40, thirdPage.Count);
        Assert.Equal("Race 101", thirdPage[0].Name);
        Assert.Equal("Race 140", thirdPage[^1].Name);
    }

    [Fact]
    public void ItraCreateCountryMessage_KeepsCoherentPageNumberAndMetadata()
    {
        var pageTwo = DiscoverItraRaces.CreateCountryMessage(page: 2, countryIndex: 0, countryPage: 2, countryPageCount: 3, racesOnPage: 50);
        var pageFour = DiscoverItraRaces.CreateCountryMessage(page: 4, countryIndex: 1, countryPage: 1);

        Assert.Equal(2, pageTwo.CurrentPage);
        Assert.Equal("AF", pageTwo.Country);
        Assert.Equal(2, pageTwo.CountryPage);
        Assert.Equal(3, pageTwo.CountryPageCount);
        Assert.Equal(50, pageTwo.RacesOnPage);

        Assert.Equal(4, pageFour.CurrentPage);
        Assert.Equal("AL", pageFour.Country);
        Assert.Equal(1, pageFour.CountryPage);
        Assert.Null(pageFour.RacesOnPage);
    }

    [Fact]
    public void ItraResolvePageContext_PrefersExplicitCountryMetadata()
    {
        var message = new RaceDiscoveryMessage("itra", Page: 4, Country: "us", CountryIndex: 226, CountryPage: 2);

        var context = DiscoverItraRaces.ResolvePageContext(message);

        Assert.Equal(226, context.CountryIndex);
        Assert.Equal("US", context.Country);
        Assert.Equal(2, context.CountryPage);
    }
}
