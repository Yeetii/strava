using System.Globalization;
using System.Text.Json;
using System.Net;
using Microsoft.Extensions.Logging;

namespace Shared.Services;

public interface ILocationGeocodingService
{
    Task<(double lat, double lng)?> GeocodeAsync(string location, string? country, CancellationToken cancellationToken);
}

public sealed class NominatimLocationGeocodingService(HttpClient httpClient, ILogger<NominatimLocationGeocodingService> logger) : ILocationGeocodingService
{
    private static readonly SemaphoreSlim RequestGate = new(1, 1);
    private static readonly TimeSpan MinimumDelayBetweenRequests = TimeSpan.FromSeconds(1);
    private static DateTimeOffset _nextAllowedRequestUtc = DateTimeOffset.MinValue;
    private static readonly Dictionary<string, (double lat, double lng)?> Cache = new(StringComparer.OrdinalIgnoreCase);
    private static readonly JsonSerializerOptions CacheJsonOptions = new(JsonSerializerDefaults.Web) { WriteIndented = true };
    private static readonly string CacheRoot = ResolveCacheRoot();
    private static int _loggedCachePath;

    private readonly HttpClient _httpClient = httpClient;
    private readonly ILogger<NominatimLocationGeocodingService> _logger = logger;

    public async Task<(double lat, double lng)?> GeocodeAsync(string location, string? country, CancellationToken cancellationToken)
    {
        LogCachePathOnce();

        if (string.IsNullOrWhiteSpace(location))
            return null;

        var normalizedLocation = location.Trim();
        if (ShouldRejectLocation(normalizedLocation))
        {
            _logger.LogInformation("Rejecting geocode query for obviously bad location '{Location}'", normalizedLocation);
            return null;
        }

        var normalizedCountry = NormalizeCountryToIso2(country)?.ToUpperInvariant() ?? string.Empty;
        var cacheKey = string.Concat(normalizedLocation, "|", normalizedCountry);

        lock (Cache)
        {
            if (Cache.TryGetValue(cacheKey, out var cached))
            {
                _logger.LogDebug(
                    "Geocode cache hit for location '{Location}' country '{Country}' => {HasCoordinates}",
                    normalizedLocation,
                    string.IsNullOrWhiteSpace(normalizedCountry) ? "(none)" : normalizedCountry,
                    cached.HasValue);
                return cached;
            }
        }

        _logger.LogDebug(
            "Geocode cache miss for location '{Location}' country '{Country}'",
            normalizedLocation,
            string.IsNullOrWhiteSpace(normalizedCountry) ? "(none)" : normalizedCountry);

        try
        {
            var query = new Dictionary<string, string>
            {
                ["q"] = normalizedLocation,
                ["format"] = "json",
                ["limit"] = "1",
                ["addressdetails"] = "0"
            };

            if (!string.IsNullOrWhiteSpace(normalizedCountry))
                query["countrycodes"] = normalizedCountry.ToLowerInvariant();

            var queryString = string.Join('&', query.Select(kvp => $"{Uri.EscapeDataString(kvp.Key)}={Uri.EscapeDataString(kvp.Value)}"));

            _logger.LogDebug(
                "Geocoding search q='{Location}' countrycodes='{CountryCodes}'",
                normalizedLocation,
                query.TryGetValue("countrycodes", out var countryCodes) ? countryCodes : "");

            using var response = await SendThrottledAsync($"search?{queryString}", cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogDebug("Geocoding request failed for '{Location}' with status {StatusCode}", location, response.StatusCode);
                UpdateCache(cacheKey, null);
                return null;
            }

            await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
            using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
            if (document.RootElement.ValueKind != JsonValueKind.Array || document.RootElement.GetArrayLength() == 0)
            {
                UpdateCache(cacheKey, null);
                return null;
            }

            var first = document.RootElement[0];
            if (!first.TryGetProperty("lat", out var latElement)
                || !first.TryGetProperty("lon", out var lonElement))
            {
                UpdateCache(cacheKey, null);
                return null;
            }

            if (TryParseNumber(latElement, out var lat)
                && TryParseNumber(lonElement, out var lng))
            {
                UpdateCache(cacheKey, (lat, lng));
                _logger.LogDebug(
                    "Geocoding resolved '{Location}' country '{Country}' => {Lat}, {Lng}",
                    normalizedLocation,
                    string.IsNullOrWhiteSpace(normalizedCountry) ? "(none)" : normalizedCountry,
                    lat,
                    lng);
                return (lat, lng);
            }

            UpdateCache(cacheKey, null);
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (TaskCanceledException ex)
        {
            _logger.LogWarning(ex,
                "Geocoding timed out for location '{Location}' country '{Country}'; treating as miss",
                normalizedLocation,
                string.IsNullOrWhiteSpace(normalizedCountry) ? "(none)" : normalizedCountry);
            UpdateCache(cacheKey, null);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to geocode location '{Location}'", location);
            UpdateCache(cacheKey, null);
            return null;
        }
    }

    private async Task<HttpResponseMessage> SendThrottledAsync(string relativeUrl, CancellationToken cancellationToken)
    {
        await RequestGate.WaitAsync(cancellationToken);
        try
        {
            var wait = _nextAllowedRequestUtc - DateTimeOffset.UtcNow;
            if (wait > TimeSpan.Zero)
            {
                _logger.LogDebug("Geocoding rate limit wait {DelayMs} ms", (int)wait.TotalMilliseconds);
                await Task.Delay(wait, cancellationToken);
            }

            using var request = new HttpRequestMessage(HttpMethod.Get, relativeUrl);
            var response = await _httpClient.SendAsync(request, cancellationToken);

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
            {
                var retryAfter = response.Headers.RetryAfter?.Delta
                    ?? (response.Headers.RetryAfter?.Date is DateTimeOffset retryAt ? retryAt - DateTimeOffset.UtcNow : (TimeSpan?)null)
                    ?? TimeSpan.FromSeconds(5);
                if (retryAfter < MinimumDelayBetweenRequests)
                    retryAfter = MinimumDelayBetweenRequests;

                _nextAllowedRequestUtc = DateTimeOffset.UtcNow.Add(retryAfter);
                _logger.LogWarning("Geocoding received 429. Backing off for {DelayMs} ms", (int)retryAfter.TotalMilliseconds);
            }
            else
            {
                _nextAllowedRequestUtc = DateTimeOffset.UtcNow.Add(MinimumDelayBetweenRequests);
            }

            return response;
        }
        finally
        {
            RequestGate.Release();
        }
    }

    private static void UpdateCache(string cacheKey, (double lat, double lng)? value)
    {
        lock (Cache)
        {
            Cache[cacheKey] = value;
            WriteCacheSnapshotUnsafe();
        }
    }

    private static void WriteCacheSnapshotUnsafe()
    {
        try
        {
            var directory = Path.Combine(CacheRoot, "__geocoding");
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, "nominatim-cache.json");

            var payload = Cache.ToDictionary(
                kvp => kvp.Key,
                kvp => kvp.Value.HasValue
                    ? new GeocodeCacheSnapshotEntry(true, kvp.Value.Value.lat, kvp.Value.Value.lng)
                    : new GeocodeCacheSnapshotEntry(false, null, null),
                StringComparer.OrdinalIgnoreCase);

            var json = JsonSerializer.Serialize(payload, CacheJsonOptions);
            WriteFileAtomically(path, json);
        }
        catch
        {
            // Cache snapshotting is best-effort and must never fail geocoding.
        }
    }

    private readonly record struct GeocodeCacheSnapshotEntry(bool HasCoordinates, double? Latitude, double? Longitude);

    private void LogCachePathOnce()
    {
        if (Interlocked.Exchange(ref _loggedCachePath, 1) == 1)
            return;

        _logger.LogInformation("Geocode cache snapshot path: {Path}", Path.Combine(CacheRoot, "__geocoding", "nominatim-cache.json"));
    }

    private static void WriteFileAtomically(string path, string content)
    {
        var tempPath = path + ".tmp";
        File.WriteAllText(tempPath, content);
        File.Move(tempPath, path, overwrite: true);
    }

    private static string ResolveCacheRoot()
    {
        var baseDir = AppContext.BaseDirectory;
        var projectRoot = Path.GetFullPath(Path.Combine(baseDir, "..", "..", ".."));
        return Path.Combine(projectRoot, ".geocoding-cache");
    }

    private static bool ShouldRejectLocation(string location)
    {
        if (string.IsNullOrWhiteSpace(location))
            return true;

        var trimmed = location.Trim();
        if (trimmed.StartsWith(",", StringComparison.Ordinal))
            return true;

        var withoutLettersOrDigits = new string(trimmed.Where(ch => char.IsLetterOrDigit(ch)).ToArray());
        if (withoutLettersOrDigits.Length == 0)
            return true;

        return false;
    }

    private static string? NormalizeCountryToIso2(string? country)
    {
        if (string.IsNullOrWhiteSpace(country))
            return null;

        var trimmed = country.Trim();
        if (trimmed.Length == 2 && trimmed.All(char.IsLetter))
            return trimmed.ToUpperInvariant();

        if (trimmed.Length == 3 && trimmed.All(char.IsLetter) && Iso3ToIso2.TryGetValue(trimmed.ToUpperInvariant(), out var fromIso3))
            return fromIso3;

        if (CountryNameToIso2.TryGetValue(trimmed.ToLowerInvariant(), out var fromName))
            return fromName;

        return null;
    }

    private static readonly Dictionary<string, string> Iso3ToIso2 = new(StringComparer.OrdinalIgnoreCase)
    {
        ["AND"] = "AD", ["ALB"] = "AL", ["AUT"] = "AT", ["BEL"] = "BE", ["BGR"] = "BG",
        ["BIH"] = "BA", ["BLR"] = "BY", ["CHE"] = "CH", ["CYP"] = "CY", ["CZE"] = "CZ",
        ["DEU"] = "DE", ["DNK"] = "DK", ["ESP"] = "ES", ["EST"] = "EE", ["FIN"] = "FI",
        ["FRA"] = "FR", ["GBR"] = "GB", ["GRC"] = "GR", ["HRV"] = "HR", ["HUN"] = "HU",
        ["IRL"] = "IE", ["ISL"] = "IS", ["ITA"] = "IT", ["KOS"] = "XK", ["LIE"] = "LI",
        ["LTU"] = "LT", ["LUX"] = "LU", ["LVA"] = "LV", ["MDA"] = "MD", ["MKD"] = "MK",
        ["MLT"] = "MT", ["MNE"] = "ME", ["NLD"] = "NL", ["NOR"] = "NO", ["POL"] = "PL",
        ["PRT"] = "PT", ["ROU"] = "RO", ["RUS"] = "RU", ["SRB"] = "RS", ["SVK"] = "SK",
        ["SVN"] = "SI", ["SWE"] = "SE", ["TUR"] = "TR", ["UKR"] = "UA",
        ["AUS"] = "AU", ["BRA"] = "BR", ["CAN"] = "CA", ["CHN"] = "CN", ["HKG"] = "HK",
        ["IDN"] = "ID", ["IND"] = "IN", ["JPN"] = "JP", ["KOR"] = "KR", ["MEX"] = "MX",
        ["MYS"] = "MY", ["NZL"] = "NZ", ["PHL"] = "PH", ["SGP"] = "SG", ["THA"] = "TH",
        ["USA"] = "US", ["ZAF"] = "ZA",
        ["ARG"] = "AR", ["COL"] = "CO", ["PER"] = "PE", ["PRY"] = "PY", ["URY"] = "UY",
        ["DOM"] = "DO", ["CHL"] = "CL", ["BGR"] = "BG", ["MAR"] = "MA"
    };

    private static readonly Dictionary<string, string> CountryNameToIso2 = new(StringComparer.OrdinalIgnoreCase)
    {
        ["andorra"] = "AD", ["albania"] = "AL", ["austria"] = "AT", ["belgium"] = "BE",
        ["bulgaria"] = "BG", ["bosnia and herzegovina"] = "BA", ["bosnia"] = "BA",
        ["belarus"] = "BY", ["switzerland"] = "CH", ["cyprus"] = "CY", ["czechia"] = "CZ",
        ["czech republic"] = "CZ", ["germany"] = "DE", ["denmark"] = "DK", ["spain"] = "ES",
        ["estonia"] = "EE", ["finland"] = "FI", ["france"] = "FR", ["united kingdom"] = "GB",
        ["uk"] = "GB", ["great britain"] = "GB", ["greece"] = "GR", ["croatia"] = "HR",
        ["hungary"] = "HU", ["ireland"] = "IE", ["iceland"] = "IS", ["italy"] = "IT",
        ["liechtenstein"] = "LI", ["lithuania"] = "LT", ["luxembourg"] = "LU", ["latvia"] = "LV",
        ["moldova"] = "MD", ["north macedonia"] = "MK", ["malta"] = "MT", ["montenegro"] = "ME",
        ["netherlands"] = "NL", ["norway"] = "NO", ["poland"] = "PL", ["portugal"] = "PT",
        ["romania"] = "RO", ["russia"] = "RU", ["serbia"] = "RS", ["slovakia"] = "SK",
        ["slovenia"] = "SI", ["sweden"] = "SE", ["turkey"] = "TR", ["ukraine"] = "UA",
        ["australia"] = "AU", ["brazil"] = "BR", ["canada"] = "CA", ["china"] = "CN",
        ["hong kong"] = "HK", ["indonesia"] = "ID", ["india"] = "IN", ["japan"] = "JP",
        ["south korea"] = "KR", ["mexico"] = "MX", ["malaysia"] = "MY", ["new zealand"] = "NZ",
        ["philippines"] = "PH", ["singapore"] = "SG", ["thailand"] = "TH", ["united states"] = "US",
        ["usa"] = "US", ["south africa"] = "ZA", ["argentina"] = "AR", ["colombia"] = "CO",
        ["peru"] = "PE", ["paraguay"] = "PY", ["uruguay"] = "UY", ["dominican republic"] = "DO",
        ["chile"] = "CL", ["morocco"] = "MA"
    };

}
