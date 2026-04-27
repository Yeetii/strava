using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Shared.Services;

public interface ILocationGeocodingService
{
    Task<(double lat, double lng)?> GeocodeAsync(string location, string? country, CancellationToken cancellationToken);
}

public sealed class NominatimLocationGeocodingService(HttpClient httpClient, ILogger<NominatimLocationGeocodingService> logger) : ILocationGeocodingService
{
    private readonly HttpClient _httpClient = httpClient;
    private readonly ILogger<NominatimLocationGeocodingService> _logger = logger;

    public async Task<(double lat, double lng)?> GeocodeAsync(string location, string? country, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(location))
            return null;

        try
        {
            var query = new Dictionary<string, string>
            {
                ["q"] = location,
                ["format"] = "json",
                ["limit"] = "1",
                ["addressdetails"] = "0"
            };

            if (!string.IsNullOrWhiteSpace(country) && country.Length == 2)
                query["countrycodes"] = country.ToLowerInvariant();

            var queryString = string.Join('&', query.Select(kvp => $"{Uri.EscapeDataString(kvp.Key)}={Uri.EscapeDataString(kvp.Value)}"));
            using var response = await _httpClient.GetAsync($"search?{queryString}", cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogDebug("Geocoding request failed for '{Location}' with status {StatusCode}", location, response.StatusCode);
                return null;
            }

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
            if (document.RootElement.ValueKind != JsonValueKind.Array || document.RootElement.GetArrayLength() == 0)
                return null;

            var first = document.RootElement[0];
            if (!first.TryGetProperty("lat", out var latElement)
                || !first.TryGetProperty("lon", out var lonElement))
            {
                return null;
            }

            if (TryParseNumber(latElement, out var lat)
                && TryParseNumber(lonElement, out var lng))
            {
                return (lat, lng);
            }

            return null;

            static bool TryParseNumber(JsonElement element, out double value)
            {
                if (element.ValueKind == JsonValueKind.Number)
                    return element.TryGetDouble(out value);

                if (element.ValueKind == JsonValueKind.String
                    && double.TryParse(element.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out value))
                {
                    return true;
                }

                value = default;
                return false;
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to geocode location '{Location}'", location);
            return null;
        }
    }
}
