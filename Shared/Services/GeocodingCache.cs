using System.Security.Cryptography;
using System.Text;
using Azure.Data.Tables;

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

internal readonly record struct GeocodingCacheKeyParts(string NormalizedKey, string PartitionKey, string RowKey)
{
    internal static GeocodingCacheKeyParts Create(string location, string countryCode)
    {
        var normalizedLocation = NormalizeLocation(location);
        var normalizedCountryCode = NormalizeCountryCode(countryCode);
        var normalizedKey = string.Concat(normalizedLocation, "|", normalizedCountryCode);

        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(normalizedKey));
        var hash = Convert.ToHexString(hashBytes);
        return new(normalizedKey, hash[..2], hash[2..]);
    }

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
