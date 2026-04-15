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
    private const string LastScrapedUtcProperty = "lastScrapedUtc";
    private static readonly Uri SourceUrl = new("https://www.loppkartan.se/markers-se.json");

    [Function(nameof(UpsertLoppkartanRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.UpsertLoppkartanRace, Connection = "ServicebusConnection", IsBatched = true)] ServiceBusReceivedMessage[] messages,
        CancellationToken cancellationToken)
    {
        var upsertedCount = 0;

        foreach (var message in messages)
        {
            LoppkartanScrapeTarget? target;
            try
            {
                target = message.Body.ToObjectFromJson<LoppkartanScrapeTarget>();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to deserialize Loppkartan race message");
                continue;
            }

            if (target is null || string.IsNullOrWhiteSpace(target.MarkerId))
                continue;

            try
            {
                var point = new Point(new Position(target.Longitude, target.Latitude));
                var properties = new Dictionary<string, dynamic>
                {
                    ["name"] = target.Name ?? target.Location ?? $"Loppkartan {target.MarkerId}",
                    ["sourceUrl"] = SourceUrl.AbsoluteUri,
                    [LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
                };

                if (!string.IsNullOrWhiteSpace(target.Website))
                    properties["website"] = target.Website;
                if (!string.IsNullOrWhiteSpace(target.Location))
                    properties["location"] = target.Location;
                if (!string.IsNullOrWhiteSpace(target.County))
                    properties["county"] = target.County;
                if (!string.IsNullOrWhiteSpace(target.RaceDate))
                    properties["raceDate"] = target.RaceDate;
                if (!string.IsNullOrWhiteSpace(target.RaceType))
                    properties["raceType"] = target.RaceType;
                if (!string.IsNullOrWhiteSpace(target.TypeLocal))
                    properties["typeLocal"] = target.TypeLocal;
                if (!string.IsNullOrWhiteSpace(target.DomainName))
                    properties["domainName"] = target.DomainName;
                if (!string.IsNullOrWhiteSpace(target.OriginCountry))
                    properties["originCountry"] = target.OriginCountry;
                if (!string.IsNullOrWhiteSpace(target.DistanceVerbose))
                    properties["distanceVerbose"] = target.DistanceVerbose;

                var featureId = $"loppkartan:{target.MarkerId}";
                var feature = new Feature(point, properties, null, new FeatureId(featureId));
                var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
                await racesCollectionClient.UpsertDocument(stored, cancellationToken);
                upsertedCount++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Failed to upsert Loppkartan marker {MarkerId}", target.MarkerId);
            }
        }

        logger.LogInformation("Loppkartan: upserted {Count}/{Total} races in batch", upsertedCount, messages.Length);
    }
}
