using Stigar.Core.Models;
using Xunit;

namespace Stigar.Core.Tests;

public sealed class UserSyncItemTests
{
    [Fact]
    public void ToUtcDateTime_ConvertsUnixMilliseconds()
    {
        var result = UserSyncItem.ToUtcDateTime(1_700_000_000_000);

        Assert.Equal(
            new DateTime(2023, 11, 14, 22, 13, 20, DateTimeKind.Utc),
            result);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(long.MaxValue)]
    public void ToUtcDateTime_ReturnsNullForMissingOrInvalidTimestamps(long? updatedAt)
    {
        Assert.Null(UserSyncItem.ToUtcDateTime(updatedAt));
    }
}
