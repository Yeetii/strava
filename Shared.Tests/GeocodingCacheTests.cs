using Shared.Services;

namespace Shared.Tests;

public sealed class GeocodingCacheTests
{
    [Fact]
    public void Create_NormalizesLocationAndCountryBeforeHashing()
    {
        var key = GeocodingCacheKeyParts.Create("  A\u030A   re  ", "SE");

        Assert.Equal("å re|se", key.NormalizedKey);
        Assert.Equal(2, key.PartitionKey.Length);
        Assert.Equal(62, key.RowKey.Length);
    }

    [Fact]
    public void Create_ProducesSameHashForEquivalentWhitespaceAndCase()
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

        Assert.False(TableGeocodingCache.IsExpired(cachedAtUtc, cachedAtUtc.AddDays(89).AddHours(23), ttl));
        Assert.True(TableGeocodingCache.IsExpired(cachedAtUtc, cachedAtUtc.AddDays(90), ttl));
    }
}
