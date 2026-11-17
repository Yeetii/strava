using System.Text.Json;
using Microsoft.Extensions.Logging;
using Moq;
using Shared.Services;
using StackExchange.Redis;

namespace Shared.Tests;

public sealed class RedisGeocodingCacheTests
{
    private static readonly DateTimeOffset Now = new(2026, 6, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task GetAsync_MissingKey_ReturnsNull()
    {
        var (cache, database) = CreateCache(Now);
        database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);

        var result = await cache.GetAsync("Åre", "SE", CancellationToken.None);

        Assert.Null(result);
        database.Verify(
            d => d.StringGetAsync((RedisKey)ExpectedKey("Åre", "SE"), It.IsAny<CommandFlags>()),
            Times.Once);
    }

    [Fact]
    public async Task SetAsync_WritesPrefixedKeyJsonAndConfiguredTtl()
    {
        var (cache, database) = CreateCache(Now, ttlDays: 7);
        RedisKey? writtenKey = null;
        string? writtenJson = null;
        TimeSpan? writtenExpiry = null;
        SetupStringSet(database, (key, value, expiry, _) =>
        {
            writtenKey = key;
            writtenJson = value.ToString();
            writtenExpiry = expiry;
        });

        var cachedAt = Now.AddMinutes(-5);
        await cache.SetAsync("  Åre  ", "SE", (59.3, 18.1), cachedAt, CancellationToken.None);

        Assert.Equal(ExpectedKey("  Åre  ", "SE"), writtenKey!.Value.ToString());
        Assert.Equal(TimeSpan.FromDays(7), writtenExpiry);
        using var json = JsonDocument.Parse(writtenJson!);
        Assert.Equal(59.3, json.RootElement.GetProperty("Latitude").GetDouble());
        Assert.Equal(18.1, json.RootElement.GetProperty("Longitude").GetDouble());
        Assert.Equal(cachedAt, json.RootElement.GetProperty("CachedAtUtc").GetDateTimeOffset());
    }

    [Fact]
    public async Task SetAsync_UsesDefaultTtlWhenConfiguredValueIsNotPositive()
    {
        var (cache, database) = CreateCache(Now, ttlDays: 0);
        TimeSpan? writtenExpiry = null;
        SetupStringSet(database, (_, _, expiry, _) => writtenExpiry = expiry);

        await cache.SetAsync("Åre", "SE", null, Now, CancellationToken.None);

        Assert.Equal(TimeSpan.FromDays(GeocodingCacheOptions.DefaultTimeToLiveDays), writtenExpiry);
    }

    [Fact]
    public async Task GetAsync_FreshEntry_ReturnsCoordinatesAndCachedAtUtc()
    {
        var (cache, database) = CreateCache(Now);
        var cachedAt = Now.AddHours(-1);
        database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(new RedisValue(CacheValue(59.3, 18.1, cachedAt)));

        var result = await cache.GetAsync("Åre", "SE", CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal<(double, double)?>((59.3, 18.1), result.Value.Coordinates);
        Assert.Equal(cachedAt, result.Value.CachedAtUtc);
    }

    [Fact]
    public async Task GetAsync_ExpiredEntry_ReturnsNull()
    {
        var (cache, database) = CreateCache(Now, ttlDays: 7);
        database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(new RedisValue(CacheValue(59.3, 18.1, Now.AddDays(-8))));

        var result = await cache.GetAsync("Åre", "SE", CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task GetAsync_CorruptJson_ReturnsNull()
    {
        var (cache, database) = CreateCache(Now);
        database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(new RedisValue("{not-json"));

        var result = await cache.GetAsync("Åre", "SE", CancellationToken.None);

        Assert.Null(result);
    }

    [Fact]
    public async Task NullCoordinates_RoundTripAsNullCoordinates()
    {
        var (cache, database) = CreateCache(Now);
        var cachedAt = Now.AddMinutes(-1);
        RedisValue written = RedisValue.Null;
        SetupStringSet(database, (_, value, _, _) => written = value);

        await cache.SetAsync("Åre", "SE", null, cachedAt, CancellationToken.None);

        database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(written);

        var result = await cache.GetAsync("Åre", "SE", CancellationToken.None);

        Assert.NotNull(result);
        Assert.Null(result.Value.Coordinates);
        Assert.Equal(cachedAt, result.Value.CachedAtUtc);
    }

    private static string ExpectedKey(string location, string countryCode)
        => "geocache:" + GeocodingCacheKeyParts.Create(location, countryCode).NormalizedKey;

    private static string CacheValue(double? lat, double? lng, DateTimeOffset cachedAtUtc)
        => JsonSerializer.Serialize(new { Latitude = lat, Longitude = lng, CachedAtUtc = cachedAtUtc });

    private static (RedisGeocodingCache Cache, Mock<IDatabase> Database) CreateCache(
        DateTimeOffset now,
        int ttlDays = 7)
    {
        var database = new Mock<IDatabase>();
        var cache = new RedisGeocodingCache(
            database.Object,
            new FakeTimeProvider(now),
            new GeocodingCacheOptions { TimeToLiveDays = ttlDays },
            new Mock<ILogger<RedisGeocodingCache>>().Object);
        return (cache, database);
    }

    private static void SetupStringSet(
        Mock<IDatabase> database,
        Action<RedisKey, RedisValue, TimeSpan?, CommandFlags> onSet)
        => database
            .Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan?>(),
                It.IsAny<When>(),
                It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, TimeSpan?, When, CommandFlags>(
                (key, value, expiry, _, flags) => onSet(key, value, expiry, flags))
            .ReturnsAsync(true);

    private sealed class FakeTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
