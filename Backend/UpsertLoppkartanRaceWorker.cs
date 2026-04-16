using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class UpsertLoppkartanRaceWorker(
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
            var point = new Point(new Position(target.Longitude, target.Latitude));
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

            var featureId = $"loppkartan:{target.MarkerId}";
            var feature = new Feature(point, properties, null, new FeatureId(featureId));
            var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
            await racesCollectionClient.UpsertDocument(stored, cancellationToken);
            logger.LogInformation("Loppkartan: upserted marker {MarkerId}", target.MarkerId);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Failed to upsert Loppkartan marker {MarkerId}", target.MarkerId);
        }
    }
}
