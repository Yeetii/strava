using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Services;
using Shared.Models;

namespace Backend;

public partial class ScrapeTrailRunningRaces(
    IHttpClientFactory httpClientFactory,
    RaceCollectionClient racesCollectionClient,
    ILogger<ScrapeTrailRunningRaces> logger)
{
    private const int Zoom = RaceCollectionClient.DefaultZoom;
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory;
    private readonly RaceCollectionClient _racesCollectionClient = racesCollectionClient;
    private readonly ILogger<ScrapeTrailRunningRaces> _logger = logger;

    [Function(nameof(ScrapeTrailRunningRaces))]
    public async Task Run(
        [TimerTrigger("0 0 0 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var sourceUrls = ParseSourceUrls(Environment.GetEnvironmentVariable("RaceScrapeSourceUrls"));
        if (sourceUrls.Count == 0)
        {
            _logger.LogWarning("No race sources configured. Set RaceScrapeSourceUrls to begin scraping.");
            return;
        }

        var httpClient = _httpClientFactory.CreateClient();
        var races = new List<StoredFeature>();

        foreach (var sourceUrl in sourceUrls)
        {
            var gpxUrls = await DiscoverGpxUrlsAsync(httpClient, sourceUrl, cancellationToken);
            foreach (var gpxUrl in gpxUrls)
            {
                try
                {
                    var gpxContent = await httpClient.GetStringAsync(gpxUrl, cancellationToken);
                    var parsedRoute = GpxParser.TryParseRoute(gpxContent);
                    if (parsedRoute is null)
                    {
                        _logger.LogWarning("Skipping GPX {GpxUrl}: failed to parse route points", gpxUrl);
                        continue;
                    }

                    var id = HashString(gpxUrl.AbsoluteUri);
                    var lineString = new LineString(parsedRoute.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
                    var feature = new Feature(
                        lineString,
                        new Dictionary<string, dynamic>
                        {
                            ["name"] = parsedRoute.Name,
                            ["sourceUrl"] = sourceUrl.AbsoluteUri,
                            ["gpxUrl"] = gpxUrl.AbsoluteUri,
                            ["gpx"] = gpxContent,
                            ["lastScrapedUtc"] = DateTime.UtcNow.ToString("o")
                        },
                        null,
                        new FeatureId(id));

                    races.Add(new StoredFeature(feature, FeatureKinds.Race, Zoom));
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Failed to fetch or parse GPX from {GpxUrl}", gpxUrl);
                }
            }
        }

        if (races.Count == 0)
        {
            _logger.LogInformation("Race scrape completed with no parsed GPX routes");
            return;
        }

        await _racesCollectionClient.BulkUpsert(races, cancellationToken);
        _logger.LogInformation("Race scrape completed. Upserted {RaceCount} routes", races.Count);
    }

    private static List<Uri> ParseSourceUrls(string? configuredUrls)
    {
        if (string.IsNullOrWhiteSpace(configuredUrls))
            return [];

        return configuredUrls
            .Split([',', ';', '\n', '\r'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(text => Uri.TryCreate(text, UriKind.Absolute, out var uri) ? uri : null)
            .Where(uri => uri is { Scheme: "http" or "https" })
            .Cast<Uri>()
            .Distinct()
            .ToList();
    }

    private async Task<IReadOnlyCollection<Uri>> DiscoverGpxUrlsAsync(HttpClient httpClient, Uri sourceUrl, CancellationToken cancellationToken)
    {
        if (sourceUrl.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
            return [sourceUrl];

        var html = await httpClient.GetStringAsync(sourceUrl, cancellationToken);
        var urls = HrefRegex()
            .Matches(html)
            .Select(match => match.Groups["href"].Value)
            .Select(href => Uri.TryCreate(sourceUrl, href, out var uri) ? uri : null)
            .Where(uri => uri is { Scheme: "http" or "https" })
            .Cast<Uri>()
            .Where(uri =>
                uri.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase)
                || uri.AbsoluteUri.Contains("gpx", StringComparison.OrdinalIgnoreCase))
            .Distinct()
            .ToList();

        return urls;
    }

    private static string HashString(string value)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }

    [GeneratedRegex("href\\s*=\\s*[\"'](?<href>[^\"']+)[\"']", RegexOptions.IgnoreCase)]
    private static partial Regex HrefRegex();
}
