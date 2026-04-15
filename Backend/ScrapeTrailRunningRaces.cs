using System.Security.Cryptography;
using System.Text;
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
    private static readonly Uri UtmbSearchApiUrl = new("https://api.utmb.world/search/races?lang=en&limit=400");
    private readonly IHttpClientFactory _httpClientFactory = httpClientFactory;
    private readonly RaceCollectionClient _racesCollectionClient = racesCollectionClient;
    private readonly ILogger<ScrapeTrailRunningRaces> _logger = logger;
    private const string LastScrapedUtcProperty = "lastScrapedUtc";

    [Function(nameof(ScrapeTrailRunningRaces))]
    public async Task Run(
        [TimerTrigger("0 0 0 * * 1")] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        var httpClient = _httpClientFactory.CreateClient();
        var races = new List<StoredFeature>();

        var scrapeTargets = await DiscoverScrapeTargetsAsync(httpClient, UtmbSearchApiUrl, cancellationToken);

        foreach (var target in scrapeTargets.GroupBy(target => target.GpxUrl.AbsoluteUri, StringComparer.OrdinalIgnoreCase).Select(group => group.First()))
        {
            try
            {
                var gpxContent = await httpClient.GetStringAsync(target.GpxUrl, cancellationToken);
                var parsedRoute = GpxParser.TryParseRoute(gpxContent, target.Name ?? "Unnamed route");
                if (parsedRoute is null)
                {
                    _logger.LogWarning("Skipping GPX {GpxUrl}: failed to parse route points", target.GpxUrl);
                    continue;
                }

                var routeId = HashString(target.GpxUrl.AbsoluteUri);
                var lineString = new LineString(parsedRoute.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
                var properties = new Dictionary<string, dynamic>
                {
                    ["name"] = parsedRoute.Name,
                    ["sourceUrl"] = target.SourceUrl.AbsoluteUri,
                    ["coursePageUrl"] = target.CoursePageUrl.AbsoluteUri,
                    ["gpxUrl"] = target.GpxUrl.AbsoluteUri,
                    ["gpx"] = gpxContent,
                    [LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
                };

                if (target.Distance.HasValue)
                    properties["distance"] = target.Distance.Value;
                if (target.ElevationGain.HasValue)
                    properties["elevationGain"] = target.ElevationGain.Value;

                var feature = new Feature(lineString, properties, null, new FeatureId(routeId));
                races.Add(new StoredFeature(feature, FeatureKinds.Race, Zoom));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Failed to fetch or parse GPX from {GpxUrl}", target.GpxUrl);
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

    private async Task<IReadOnlyCollection<RaceScrapeTarget>> DiscoverScrapeTargetsAsync(HttpClient httpClient, Uri sourceUrl, CancellationToken cancellationToken)
    {
        if (sourceUrl.AbsolutePath.EndsWith(".gpx", StringComparison.OrdinalIgnoreCase))
        {
            return [new RaceScrapeTarget(sourceUrl, sourceUrl, sourceUrl, null, null, null)];
        }

        if (RaceScrapeDiscovery.IsUtmbSearchApi(sourceUrl))
        {
            var json = await httpClient.GetStringAsync(sourceUrl, cancellationToken);
            var pages = RaceScrapeDiscovery.ParseUtmbRacePages(json);
            var targets = new List<RaceScrapeTarget>();

            foreach (var page in pages)
            {
                try
                {
                    var html = await httpClient.GetStringAsync(page.PageUrl, cancellationToken);
                    var gpxUrls = RaceScrapeDiscovery.ExtractGpxUrlsFromHtml(html, page.PageUrl);
                    targets.AddRange(gpxUrls.Select(gpxUrl =>
                        new RaceScrapeTarget(gpxUrl, sourceUrl, page.PageUrl, page.Name, page.Distance, page.ElevationGain)));
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Failed to discover GPX links from race page {RacePageUrl}", page.PageUrl);
                }
            }

            return targets;
        }

        var sourceHtml = await httpClient.GetStringAsync(sourceUrl, cancellationToken);
        var discoveredGpxUrls = RaceScrapeDiscovery.ExtractGpxUrlsFromHtml(sourceHtml, sourceUrl);
        return discoveredGpxUrls
            .Select(gpxUrl => new RaceScrapeTarget(gpxUrl, sourceUrl, sourceUrl, null, null, null))
            .ToList();
    }

    private static string HashString(string value)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(value));
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}

public record RaceScrapeTarget(
    Uri GpxUrl,
    Uri SourceUrl,
    Uri CoursePageUrl,
    string? Name,
    double? Distance,
    double? ElevationGain);
