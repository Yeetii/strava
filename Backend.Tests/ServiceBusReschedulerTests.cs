namespace Backend.Tests;

public class ServiceBusReschedulerTests
{
    [Fact]
    public void BuildScheduledMessageId_ReturnsSameId_ForSameSourceAndRetry()
    {
        var first = ServiceBusRescheduler.BuildScheduledMessageId("message-1", 2);
        var second = ServiceBusRescheduler.BuildScheduledMessageId("message-1", 2);

        Assert.Equal(first, second);
    }

    [Fact]
    public void BuildScheduledMessageId_ReturnsDifferentId_ForDifferentRetryCount()
    {
        var first = ServiceBusRescheduler.BuildScheduledMessageId("message-1", 1);
        var second = ServiceBusRescheduler.BuildScheduledMessageId("message-1", 2);

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void BuildScheduledMessageId_HandlesEmptySourceMessageId()
    {
        var id = ServiceBusRescheduler.BuildScheduledMessageId(string.Empty, 0);

        Assert.StartsWith("resched:0:", id);
    }

    [Fact]
    public void ShouldDeadLetter_WhenDeliveryCountHitsMaxRetryCount()
    {
        var shouldDeadLetter = ServiceBusRescheduler.ShouldDeadLetter(retryCount: 0, deliveryCount: 3, maxRetryCount: 3, hasExplicitScheduledRetry: false);

        Assert.True(shouldDeadLetter);
    }

    [Fact]
    public void ShouldDeadLetter_DoesNotUseRetryCount_WhenExplicitScheduleIsPresent()
    {
        var shouldDeadLetter = ServiceBusRescheduler.ShouldDeadLetter(retryCount: 3, deliveryCount: 1, maxRetryCount: 3, hasExplicitScheduledRetry: true);

        Assert.False(shouldDeadLetter);
    }

    [Fact]
    public void BuildRetryDeadLetterReason_UsesDeliveryLimitPrefix()
    {
        var reason = ServiceBusRescheduler.BuildRetryDeadLetterReason(new InvalidOperationException("boom"), deliveryLimitReached: true);

        Assert.Equal("DeliveryLimitExceeded_InvalidOperationException", reason);
    }
}
