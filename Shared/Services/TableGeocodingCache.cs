using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using Shared.Storage;

namespace Shared.Services;

public sealed class TableGeocodingCache(
    TableClient tableClient,
    TimeProvider timeProvider,
    GeocodingCacheOptions options,
    ILogger<TableGeocodingCache> logger) : IGeocodingCache
{
    private readonly TableClient _tableClient = tableClient;
    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly TimeSpan _timeToLive = TimeSpan.FromDays(options.TimeToLiveDays > 0 ? options.TimeToLiveDays : GeocodingCacheOptions.DefaultTimeToLiveDays);
    private readonly ILogger<TableGeocodingCache> _logger = logger;

    public async Task<GeocodeCacheEntry?> GetAsync(
        string location,
        string countryCode,
        CancellationToken cancellationToken)
    {
        var key = GeocodingCacheKeyParts.Create(location, countryCode);
        var response = await _tableClient.GetEntityIfExistsAsync<GeocodingCacheEntity>(
            key.PartitionKey,
            key.RowKey,
            cancellationToken: cancellationToken);

        if (!response.HasValue)
        {
            _logger.LogDebug(
                "Geocoding shared cache miss for location '{Location}' country '{CountryCode}'",
                location,
                string.IsNullOrWhiteSpace(countryCode) ? "(none)" : countryCode);
            return null;
        }

        var entity = response.Value;
        if (entity is null)
            return null;

        var nowUtc = _timeProvider.GetUtcNow();
        if (IsExpired(entity.CachedAtUtc, nowUtc, _timeToLive))
        {
            _logger.LogInformation(
                "Geocoding shared cache entry expired; refreshing from Nominatim for location '{Location}' country '{CountryCode}'",
                entity.Location,
                string.IsNullOrWhiteSpace(entity.CountryCode) ? "(none)" : entity.CountryCode);
            return null;
        }

        _logger.LogDebug(
            "Geocoding shared cache hit for location '{Location}' country '{CountryCode}' => {HasCoordinates}",
            entity.Location,
            string.IsNullOrWhiteSpace(entity.CountryCode) ? "(none)" : entity.CountryCode,
            entity.HasCoordinates);

        return new(
            entity.HasCoordinates && entity.Latitude.HasValue && entity.Longitude.HasValue
                ? (entity.Latitude.Value, entity.Longitude.Value)
                : null,
            entity.CachedAtUtc);
    }

    public Task SetAsync(
        string location,
        string countryCode,
        (double lat, double lng)? coordinates,
        DateTimeOffset cachedAtUtc,
        CancellationToken cancellationToken)
    {
        var key = GeocodingCacheKeyParts.Create(location, countryCode);
        var entity = new GeocodingCacheEntity
        {
            PartitionKey = key.PartitionKey,
            RowKey = key.RowKey,
            Location = location,
            CountryCode = countryCode,
            HasCoordinates = coordinates.HasValue,
            Latitude = coordinates?.lat,
            Longitude = coordinates?.lng,
            CachedAtUtc = cachedAtUtc
        };

        return _tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken);
    }

    internal static bool IsExpired(DateTimeOffset cachedAtUtc, DateTimeOffset nowUtc, TimeSpan timeToLive)
        => cachedAtUtc + timeToLive <= nowUtc;
}

public sealed class GeocodingCacheOptions
{
    public const int DefaultTimeToLiveDays = 90;
    public const string DefaultTableName = "GeocodingCache";

    public string TableName { get; set; } = DefaultTableName;
    public int TimeToLiveDays { get; set; } = DefaultTimeToLiveDays;
}
