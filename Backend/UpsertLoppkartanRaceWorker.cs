using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class UpsertLoppkartanRaceWorker(
    IHttpClientFactory httpClientFactory,
    RaceCollectionClient racesCollectionClient,
    ILogger<UpsertLoppkartanRaceWorker> logger)
{
    private const int Zoom = RaceCollectionClient.DefaultZoom;
    private static readonly Uri SourceUrl = new("https://www.loppkartan.se/markers-se.json");

    [Function(nameof(UpsertLoppkartanRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.UpsertLoppkartanRace, Connection = "ServicebusConnection")] ServiceBusReceivedMessage message,
        CancellationToken cancellationToken)
    {
        LoppkartanScrapeTarget? target;
        try
        {
            target = message.Body.ToObjectFromJson<LoppkartanScrapeTarget>();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to deserialize Loppkartan race message");
            return;
        }

        if (target is null || string.IsNullOrWhiteSpace(target.MarkerId))
            return;

        try
        {
            var properties = new Dictionary<string, dynamic>
            {
                [RaceScrapeDiscovery.PropName] = target.Name ?? target.Location ?? $"Loppkartan {target.MarkerId}",
                ["sourceUrl"] = SourceUrl.AbsoluteUri,
                [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
            };

            if (!string.IsNullOrWhiteSpace(target.Website))
                properties[RaceScrapeDiscovery.PropWebsite] = target.Website;
            if (!string.IsNullOrWhiteSpace(target.Location))
                properties[RaceScrapeDiscovery.PropLocation] = target.Location;
            if (!string.IsNullOrWhiteSpace(target.County))
                properties["county"] = target.County;
            var normalizedDate = RaceScrapeDiscovery.NormalizeDateToYyyyMmDd(target.RaceDate);
            if (!string.IsNullOrWhiteSpace(normalizedDate))
                properties[RaceScrapeDiscovery.PropDate] = normalizedDate;
            var normalizedRaceType = RaceScrapeDiscovery.NormalizeRaceType(target.RaceType);
            if (!string.IsNullOrWhiteSpace(normalizedRaceType))
                properties[RaceScrapeDiscovery.PropRaceType] = normalizedRaceType;
            if (!string.IsNullOrWhiteSpace(target.TypeLocal))
                properties["typeLocal"] = target.TypeLocal;
            if (!string.IsNullOrWhiteSpace(target.DomainName))
                properties["domainName"] = target.DomainName;
            var normalizedCountry = RaceScrapeDiscovery.NormalizeCountryToIso2(target.OriginCountry);
            if (!string.IsNullOrWhiteSpace(normalizedCountry))
                properties[RaceScrapeDiscovery.PropCountry] = normalizedCountry;
            var normalizedDistance = RaceScrapeDiscovery.ParseDistanceVerbose(target.DistanceVerbose);
            if (!string.IsNullOrWhiteSpace(normalizedDistance))
                properties[RaceScrapeDiscovery.PropDistance] = normalizedDistance;

            var baseFeatureId = $"loppkartan:{target.MarkerId}";

            // Attempt to enrich with GPX routes from the event website.
            if (!string.IsNullOrWhiteSpace(target.Website) &&
                Uri.TryCreate(target.Website, UriKind.Absolute, out var websiteUri) &&
                websiteUri.Scheme is "http" or "https")
            {
                var routes = await TryFetchWebsiteGpxRoutes(httpClientFactory.CreateClient(), websiteUri, cancellationToken);
                if (routes.Count > 0)
                {
                    for (int i = 0; i < routes.Count; i++)
                    {
                        var (route, gpxUrl) = routes[i];
                        var routeProps = new Dictionary<string, dynamic>(properties);
                        if (!string.IsNullOrWhiteSpace(route.Name))
                            routeProps[RaceScrapeDiscovery.PropName] = route.Name;
                        routeProps["gpxUrl"] = gpxUrl.AbsoluteUri;

                        var gpxDistanceKm = GpxParser.CalculateDistanceKm(route.Coordinates);
                        var matchedDistance = RaceScrapeDiscovery.MatchDistanceKmToVerbose(gpxDistanceKm, target.DistanceVerbose);
                        if (!string.IsNullOrWhiteSpace(matchedDistance))
                            routeProps[RaceScrapeDiscovery.PropDistance] = matchedDistance;
                        else if (gpxDistanceKm > 0)
                            routeProps[RaceScrapeDiscovery.PropDistance] = RaceScrapeDiscovery.FormatDistanceKm(gpxDistanceKm);

                        var routeFeatureId = routes.Count == 1 ? baseFeatureId : $"{baseFeatureId}:{i}";
                        var lineString = new LineString(route.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
                        var feature = new Feature(lineString, routeProps, null, new FeatureId(routeFeatureId));
                        var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
                        await racesCollectionClient.UpsertDocument(stored, cancellationToken);
                        logger.LogInformation("Loppkartan: upserted GPX route {RouteFeatureId} from {GpxUrl}", routeFeatureId, gpxUrl);
                    }

                    return;
                }
            }

            // Fallback: store the scraped point position.
            var point = new Point(new Position(target.Longitude, target.Latitude));
            var pointFeature = new Feature(point, properties, null, new FeatureId(baseFeatureId));
            var pointStored = new StoredFeature(pointFeature, FeatureKinds.Race, Zoom);
            await racesCollectionClient.UpsertDocument(pointStored, cancellationToken);
            logger.LogInformation("Loppkartan: upserted marker {MarkerId}", target.MarkerId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Failed to upsert Loppkartan marker {MarkerId}", target.MarkerId);
        }
    }

    private async Task<IReadOnlyList<(ParsedGpxRoute Route, Uri GpxUrl)>> TryFetchWebsiteGpxRoutes(
        HttpClient httpClient,
        Uri websiteUri,
        CancellationToken cancellationToken)
    {
        string html;
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            html = await httpClient.GetStringAsync(websiteUri, cts.Token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogDebug(ex, "Loppkartan: could not fetch website {WebsiteUri}", websiteUri);
            return [];
        }

        var gpxUrls = RaceScrapeDiscovery.ExtractGpxUrlsFromHtml(html, websiteUri);
        if (gpxUrls.Count == 0)
            return [];

        var routes = new List<(ParsedGpxRoute, Uri)>();
        foreach (var gpxUrl in gpxUrls)
        {
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(TimeSpan.FromSeconds(30));
                var gpxContent = await httpClient.GetStringAsync(gpxUrl, cts.Token);
                var parsed = GpxParser.TryParseRoute(gpxContent);
                if (parsed is not null)
                    routes.Add((parsed, gpxUrl));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogDebug(ex, "Loppkartan: could not fetch GPX {GpxUrl}", gpxUrl);
            }
        }

        return routes;
    }
}
