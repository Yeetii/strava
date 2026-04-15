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

    [Function(nameof(UpsertUtmbRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.UpsertUtmbRace, Connection = "ServicebusConnection", IsBatched = true)] ServiceBusReceivedMessage[] messages,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var upsertedCount = 0;

        foreach (var message in messages)
        {
            RaceScrapeTarget? target;
            try
            {
                target = message.Body.ToObjectFromJson<RaceScrapeTarget>();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to deserialize UTMB race message");
                continue;
            }

            if (target is null) continue;

            try
            {
                var gpxContent = await httpClient.GetStringAsync(target.GpxUrl, cancellationToken);
                var parsedRoute = GpxParser.TryParseRoute(gpxContent, target.Name ?? "Unnamed route");
                if (parsedRoute is null)
                {
                    logger.LogWarning("Skipping GPX {GpxUrl}: failed to parse route points", target.GpxUrl);
                    continue;
                }

                var routeId = RaceScrapeDiscovery.BuildUtmbFeatureId(target.CoursePageUrl);
                var lineString = new LineString(parsedRoute.Coordinates.Select(c => new Position(c.Lng, c.Lat)).ToList());
                var properties = new Dictionary<string, dynamic>
                {
                    ["name"] = parsedRoute.Name,
                    ["sourceUrl"] = target.SourceUrl.AbsoluteUri,
                    ["coursePageUrl"] = target.CoursePageUrl.AbsoluteUri,
                    ["gpxUrl"] = target.GpxUrl.AbsoluteUri,
                    ["gpx"] = gpxContent,
                    [RaceScrapeDiscovery.LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
                };

                if (target.Distance.HasValue)
                    properties["distance"] = target.Distance.Value;
                if (target.ElevationGain.HasValue)
                    properties["elevationGain"] = target.ElevationGain.Value;

                var feature = new Feature(lineString, properties, null, new FeatureId(routeId));
                var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
                await racesCollectionClient.UpsertDocument(stored, cancellationToken);
                upsertedCount++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Failed to fetch or parse GPX from {GpxUrl}", target.GpxUrl);
            }
        }

        logger.LogInformation("UTMB: upserted {Count}/{Total} races in batch", upsertedCount, messages.Length);
    }
}
