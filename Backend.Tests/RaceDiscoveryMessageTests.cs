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
        Assert.Contains("\"agent\":\"duv\"", message.Body.ToString(), StringComparison.Ordinal);
        Assert.Contains("\"page\":3", message.Body.ToString(), StringComparison.Ordinal);
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
    public void TraceDeTrailPage_CoversOneMonthAtATime()
    {
        var firstPage = DiscoverTraceDeTrailRaces.MonthForPage(1);
        var secondPage = DiscoverTraceDeTrailRaces.MonthForPage(2);

        Assert.Equal(firstPage.AddMonths(1).Year, secondPage.Year);
        Assert.Equal(firstPage.AddMonths(1).Month, secondPage.Month);
    }
}
