using Stigar.Core.Services;

namespace Stigar.Core.Tests;

public sealed class GeocodingCacheTests
{
    [Fact]
    public void Create_NormalizesLocationAndCountryBeforeBuildingKey()
    {
        var key = GeocodingCacheKeyParts.Create("  A\u030A   re  ", "SE");

        Assert.Equal("å re|se", key.NormalizedKey);
    }

    [Fact]
    public void Create_ProducesSameKeyForEquivalentWhitespaceAndCase()
    {
        var first = GeocodingCacheKeyParts.Create("Åre", "SE");
        var second = GeocodingCacheKeyParts.Create("  åre  ", "se");

        Assert.Equal(first, second);
    }

    [Fact]
    public void IsExpired_ReturnsTrueWhenEntryIsAtOrBeyondTimeToLive()
    {
        var cachedAtUtc = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var ttl = TimeSpan.FromDays(90);

        Assert.False(GeocodingCacheExpiry.IsExpired(cachedAtUtc, cachedAtUtc.AddDays(89).AddHours(23), ttl));
        Assert.True(GeocodingCacheExpiry.IsExpired(cachedAtUtc, cachedAtUtc.AddDays(90), ttl));
    }
}
