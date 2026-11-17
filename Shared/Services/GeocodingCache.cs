using System.Text;

namespace Shared.Services;

public interface IGeocodingCache
{
    Task<GeocodeCacheEntry?> GetAsync(
        string location,
        string countryCode,
        CancellationToken cancellationToken);

    Task SetAsync(
        string location,
        string countryCode,
        (double lat, double lng)? coordinates,
        DateTimeOffset cachedAtUtc,
        CancellationToken cancellationToken);
}

public readonly record struct GeocodeCacheEntry((double lat, double lng)? Coordinates, DateTimeOffset CachedAtUtc);

internal readonly record struct GeocodingCacheKeyParts(string NormalizedKey)
{
    internal static GeocodingCacheKeyParts Create(string location, string countryCode)
        => new(string.Concat(NormalizeLocation(location), "|", NormalizeCountryCode(countryCode)));

    private static string NormalizeLocation(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var normalized = value.Trim().Normalize(NormalizationForm.FormKC).ToLowerInvariant();
        return string.Join(' ', normalized.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    }

    private static string NormalizeCountryCode(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        return value.Trim().Normalize(NormalizationForm.FormKC).ToLowerInvariant();
    }
}

internal static class GeocodingCacheExpiry
{
    internal static bool IsExpired(DateTimeOffset cachedAtUtc, DateTimeOffset nowUtc, TimeSpan timeToLive)
        => cachedAtUtc + timeToLive <= nowUtc;
}

public sealed class GeocodingCacheOptions
{
    public const int DefaultTimeToLiveDays = 90;

    public int TimeToLiveDays { get; set; } = DefaultTimeToLiveDays;
}
