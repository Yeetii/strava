using Shared.Constants;

namespace Shared.Tests;

public class WinterSeasonTests
{
    [Theory]
    [InlineData(2026, 11, 1, true)]
    [InlineData(2026, 12, 31, true)]
    [InlineData(2027, 1, 1, true)]
    [InlineData(2027, 5, 9, true)]
    [InlineData(2027, 5, 10, false)]
    [InlineData(2026, 10, 31, false)]
    public void IsInSeason_UsesConfiguredWinterWindow(int year, int month, int day, bool expected)
    {
        var date = new DateTime(year, month, day, 12, 0, 0, DateTimeKind.Utc);

        var result = WinterSeason.IsInSeason(date);

        Assert.Equal(expected, result);
    }
}
