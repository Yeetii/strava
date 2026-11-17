using System.Text.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Shared.Services;

public sealed class RedisGeocodingCache(
    IDatabase database,
    TimeProvider timeProvider,
    GeocodingCacheOptions options,
    ILogger<RedisGeocodingCache> logger) : IGeocodingCache
{
    private const string KeyPrefix = "geocache:";

    private readonly IDatabase _database = database;
    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly TimeSpan _timeToLive = TimeSpan.FromDays(options.TimeToLiveDays > 0 ? options.TimeToLiveDays : GeocodingCacheOptions.DefaultTimeToLiveDays);
    private readonly ILogger<RedisGeocodingCache> _logger = logger;

    public async Task<GeocodeCacheEntry?> GetAsync(
        string location,
        string countryCode,
        CancellationToken cancellationToken)
    {
        // StackExchange.Redis async commands do not accept a CancellationToken.
        var value = await _database.StringGetAsync(BuildKey(location, countryCode), CommandFlags.None);
        if (!value.HasValue)
        {
            _logger.LogDebug(
                "Geocoding shared cache miss for location '{Location}' country '{CountryCode}'",
                location,
                string.IsNullOrWhiteSpace(countryCode) ? "(none)" : countryCode);
            return null;
        }

        RedisGeocodingCacheEntry? cached;
        try
        {
            cached = JsonSerializer.Deserialize<RedisGeocodingCacheEntry>(value.ToString());
        }
        catch (JsonException)
        {
            cached = null;
        }

        if (cached is null)
        {
            _logger.LogInformation(
                "Geocoding shared cache entry corrupt; refreshing from Nominatim for location '{Location}' country '{CountryCode}'",
                location,
                string.IsNullOrWhiteSpace(countryCode) ? "(none)" : countryCode);
            return null;
        }

        var nowUtc = _timeProvider.GetUtcNow();
        if (TableGeocodingCache.IsExpired(cached.CachedAtUtc, nowUtc, _timeToLive))
        {
            _logger.LogInformation(
                "Geocoding shared cache entry expired; refreshing from Nominatim for location '{Location}' country '{CountryCode}'",
                location,
                string.IsNullOrWhiteSpace(countryCode) ? "(none)" : countryCode);
            return null;
        }

        (double lat, double lng)? coordinates = cached.Latitude is { } lat && cached.Longitude is { } lng
            ? (lat, lng)
            : null;
        _logger.LogDebug(
            "Geocoding shared cache hit for location '{Location}' country '{CountryCode}' => {HasCoordinates}",
            location,
            string.IsNullOrWhiteSpace(countryCode) ? "(none)" : countryCode,
            coordinates.HasValue);

        return new GeocodeCacheEntry(coordinates, cached.CachedAtUtc);
    }

    public Task SetAsync(
        string location,
        string countryCode,
        (double lat, double lng)? coordinates,
        DateTimeOffset cachedAtUtc,
        CancellationToken cancellationToken)
    {
        // StackExchange.Redis async commands do not accept a CancellationToken.
        var value = JsonSerializer.Serialize(new RedisGeocodingCacheEntry(
            coordinates?.lat,
            coordinates?.lng,
            cachedAtUtc));

        return _database.StringSetAsync(
            BuildKey(location, countryCode),
            value,
            _timeToLive,
            When.Always,
            CommandFlags.None);
    }

    private static RedisKey BuildKey(string location, string countryCode)
        => new(string.Concat(KeyPrefix, GeocodingCacheKeyParts.Create(location, countryCode).NormalizedKey));

    private sealed record RedisGeocodingCacheEntry(double? Latitude, double? Longitude, DateTimeOffset CachedAtUtc);
}
