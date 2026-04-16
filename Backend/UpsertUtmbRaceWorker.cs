using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class UpsertUtmbRaceWorker(
    IHttpClientFactory httpClientFactory,
    RaceCollectionClient racesCollectionClient,
    ILogger<UpsertUtmbRaceWorker> logger)
{
    private const int Zoom = RaceCollectionClient.DefaultZoom;
    private static readonly Uri UtmbSearchApiUrl = new("https://api.utmb.world/search/races?lang=en&limit=400");

    [Function(nameof(UpsertUtmbRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.UpsertUtmbRace, Connection = "ServicebusConnection")] ServiceBusReceivedMessage message,
        CancellationToken cancellationToken)
    {
        RacePageCandidate? page;
        try
        {
            page = message.Body.ToObjectFromJson<RacePageCandidate>();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to deserialize UTMB race message");
            return;
        }

        if (page is null) return;

        try
        {
            var httpClient = httpClientFactory.CreateClient();

            string html;
            try
            {
                html = await httpClient.GetStringAsync(page.PageUrl, cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "UTMB: failed to fetch race page {PageUrl}", page.PageUrl);
                return;
            }

            var gpxUrls = RaceScrapeDiscovery.ExtractGpxUrlsFromHtml(html, page.PageUrl)
                .GroupBy(u => u.AbsoluteUri, StringComparer.OrdinalIgnoreCase)
                .Select(g => g.First())
                .ToList();

            if (gpxUrls.Count == 0)
            {
                logger.LogWarning("UTMB: no GPX links found on race page {PageUrl}", page.PageUrl);
                return;
            }

            foreach (var gpxUrl in gpxUrls)
                await TryUpsertGpxRouteAsync(httpClient, gpxUrl, page, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "UTMB: failed to process race page {PageUrl}", page.PageUrl);
        }
    }

    private async Task TryUpsertGpxRouteAsync(HttpClient httpClient, Uri gpxUrl, RacePageCandidate page, CancellationToken cancellationToken)
    {
        try
        {
            var gpxContent = await httpClient.GetStringAsync(gpxUrl, cancellationToken);
            var parsedRoute = GpxParser.TryParseRoute(gpxContent, page.Name ?? "Unnamed route");
            if (parsedRoute is null)
            {
                logger.LogWarning("UTMB: skipping GPX {GpxUrl}: failed to parse route points", gpxUrl);
                return;
            }

            var routeId = RaceScrapeDiscovery.BuildUtmbFeatureId(page.PageUrl);
            var lineString = new LineString(parsedRoute.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
            var properties = new Dictionary<string, dynamic>
            {
                [RaceScrapeDiscovery.PropName] = parsedRoute.Name,
                ["sourceUrl"] = UtmbSearchApiUrl.AbsoluteUri,
                ["coursePageUrl"] = page.PageUrl.AbsoluteUri,
                ["gpxUrl"] = gpxUrl.AbsoluteUri,
                ["gpx"] = gpxContent,
                [RaceScrapeDiscovery.PropWebsite] = page.PageUrl.AbsoluteUri,
                [RaceScrapeDiscovery.PropRaceType] = "trail",
                [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
            };

            if (page.Distance.HasValue)
                properties[RaceScrapeDiscovery.PropDistance] = RaceScrapeDiscovery.FormatDistanceKm(page.Distance.Value);
            if (page.ElevationGain.HasValue)
                properties[RaceScrapeDiscovery.PropElevationGain] = page.ElevationGain.Value;
            var normalizedCountry = RaceScrapeDiscovery.NormalizeCountryToIso2(page.Country);
            if (!string.IsNullOrWhiteSpace(normalizedCountry))
                properties[RaceScrapeDiscovery.PropCountry] = normalizedCountry;
            if (!string.IsNullOrWhiteSpace(page.Location))
                properties[RaceScrapeDiscovery.PropLocation] = page.Location;
            if (page.Playgrounds is { Count: > 0 })
                properties[RaceScrapeDiscovery.PropPlaygrounds] = page.Playgrounds;
            if (page.RunningStones is { Count: > 0 })
                properties[RaceScrapeDiscovery.PropRunningStones] = page.RunningStones;
            if (!string.IsNullOrWhiteSpace(page.ImageUrl))
                properties[RaceScrapeDiscovery.PropImage] = page.ImageUrl;

            var feature = new Feature(lineString, properties, null, new FeatureId(routeId));
            var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
            await racesCollectionClient.UpsertDocument(stored, cancellationToken);
            logger.LogInformation("UTMB: upserted race {RouteId}", routeId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "UTMB: failed to fetch or parse GPX from {GpxUrl}", gpxUrl);
        }
    }
}
