using Shared.Services;

namespace Shared.Tests;

public class CollectionClientWriteThrottleTests
{
    [Fact]
    public void GetDelayAfterWrite_UsesLongerDelayForLowPriorityWrites()
    {
        var low = CosmosWriteThrottle.GetDelayAfterWrite(CosmosWritePriority.Low);
        var normal = CosmosWriteThrottle.GetDelayAfterWrite(CosmosWritePriority.Normal);
        var high = CosmosWriteThrottle.GetDelayAfterWrite(CosmosWritePriority.High);

        Assert.True(low > normal);
        Assert.Equal(normal, high);
    }
}
