using Azure.Messaging.ServiceBus;
using Backend;

namespace Backend.Tests;

public class RaceDiscoveryMessageTests
{
    [Fact]
    public void BuildDiscoveryServiceBusMessage_WritesJsonMessage()
    {
        var message = RaceDiscoveryService.BuildDiscoveryServiceBusMessage(new RaceDiscoveryMessage("duv", 3));

        Assert.Equal("application/json", message.ContentType);
        Assert.Equal("discovery:duv:3", message.MessageId);
        Assert.Contains("\"agent\":\"duv\"", message.Body.ToString(), StringComparison.Ordinal);
        Assert.Contains("\"page\":3", message.Body.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildDiscoveryServiceBusMessage_WritesMetadataProperties()
    {
        var message = RaceDiscoveryService.BuildDiscoveryServiceBusMessage(
            new RaceDiscoveryMessage("itra", 2, Country: "AF", CountryIndex: 0, CountryPage: 2, CountryPageCount: 3, RacesOnPage: 50));

        Assert.Equal("AF", message.ApplicationProperties["country"]);
        Assert.Equal(0, message.ApplicationProperties["countryIndex"]);
        Assert.Equal(2, message.ApplicationProperties["countryPage"]);
        Assert.Equal(3, message.ApplicationProperties["countryPageCount"]);
        Assert.Equal(50, message.ApplicationProperties["racesOnPage"]);
        Assert.Contains("\"country\":\"AF\"", message.Body.ToString(), StringComparison.Ordinal);
        Assert.Contains("\"racesOnPage\":50", message.Body.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildDiscoveryServiceBusMessage_UsesStableIdAcrossRetriesForSamePage()
    {
        var first = RaceDiscoveryService.BuildDiscoveryServiceBusMessage(
            new RaceDiscoveryMessage("itra", 7, Country: "US", CountryIndex: 226, CountryPage: 3, CountryPageCount: 4, RacesOnPage: 40));
        var second = RaceDiscoveryService.BuildDiscoveryServiceBusMessage(
            new RaceDiscoveryMessage("itra", 7, Country: "US", CountryIndex: 226, CountryPage: 3, CountryPageCount: 4, RacesOnPage: 40));

        Assert.Equal("discovery:itra:7", first.MessageId);
        Assert.Equal(first.MessageId, second.MessageId);
    }

    [Fact]
    public void BuildDiscoveryServiceBusMessage_UsesDifferentIdForDifferentPages()
    {
        var first = RaceDiscoveryService.BuildDiscoveryServiceBusMessage(new RaceDiscoveryMessage("itra", 7));
        var second = RaceDiscoveryService.BuildDiscoveryServiceBusMessage(new RaceDiscoveryMessage("itra", 8));

        Assert.NotEqual(first.MessageId, second.MessageId);
    }

    [Fact]
    public void DeserializeMessage_DefaultsMissingPageToFirstPage()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"agent":"tracedetrail"}"""));

        var message = RaceDiscoveryWorker.DeserializeMessage(received);

        Assert.NotNull(message);
        Assert.Equal("tracedetrail", message.Agent);
        Assert.Null(message.Page);
        Assert.Equal(1, message.CurrentPage);
    }

    [Fact]
    public void DeserializeMessage_AcceptsCamelCasePage()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"agent":"duv","page":4}"""));

        var message = RaceDiscoveryWorker.DeserializeMessage(received);

        Assert.NotNull(message);
        Assert.Equal("duv", message.Agent);
        Assert.Equal(4, message.Page);
        Assert.Equal(4, message.CurrentPage);
    }

    [Fact]
    public void DeserializeMessage_AcceptsItraMetadata()
    {
        var received = ServiceBusModelFactory.ServiceBusReceivedMessage(
            body: BinaryData.FromString("""{"agent":"itra","page":7,"country":"US","countryIndex":226,"countryPage":3,"countryPageCount":4,"racesOnPage":40}"""));

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
