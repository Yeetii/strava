using Azure.Messaging.ServiceBus;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class UpsertTraceDeTrailRaceWorker(
    IHttpClientFactory httpClientFactory,
    RaceCollectionClient racesCollectionClient,
    ILogger<UpsertTraceDeTrailRaceWorker> logger)
{
    private const int Zoom = RaceCollectionClient.DefaultZoom;
    private const string BaseUrl = "https://tracedetrail.fr";
    private const string LastScrapedUtcProperty = "lastScrapedUtc";

    [Function(nameof(UpsertTraceDeTrailRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.UpsertTraceDeTrailRace, Connection = "ServicebusConnection", IsBatched = true)] ServiceBusReceivedMessage[] messages,
        CancellationToken cancellationToken)
    {
        var httpClient = httpClientFactory.CreateClient();
        var upsertedCount = 0;

        foreach (var message in messages)
        {
            TraceDeTrailScrapeTarget? target;
            try
            {
                target = message.Body.ToObjectFromJson<TraceDeTrailScrapeTarget>();
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to deserialize TraceDeTrail trace message");
                continue;
            }

            if (target is null) continue;

            try
            {
                var url = $"{BaseUrl}/trace/getTraceItra/{target.TraceId}";
                var json = await httpClient.GetStringAsync(url, cancellationToken);
                var traceData = RaceScrapeDiscovery.ParseTraceDeTrailTrace(json);

                if (traceData.Points.Count < 2)
                {
                    logger.LogWarning("TraceDeTrail trace {TraceId} returned fewer than 2 points, skipping", target.TraceId);
                    continue;
                }

                var lineString = new LineString(traceData.Points.Select(p => new Position(p.Lng, p.Lat)).ToList());
                var properties = new Dictionary<string, dynamic>
                {
                    ["name"] = target.Name ?? $"TraceDeTrail {target.TraceId}",
                    ["sourceUrl"] = url,
                    [LastScrapedUtcProperty] = DateTime.UtcNow.ToString("o")
                };

                var distance = target.Distance ?? traceData.TotalDistanceKm;
                if (distance.HasValue)
                    properties["distance"] = distance.Value;
                if (traceData.ElevationGain.HasValue)
                    properties["elevationGain"] = traceData.ElevationGain.Value;

                var featureId = $"tracedetrail:{target.TraceId}";
                var feature = new Feature(lineString, properties, null, new FeatureId(featureId));
                var stored = new StoredFeature(feature, FeatureKinds.Race, Zoom);
                await racesCollectionClient.UpsertDocument(stored, cancellationToken);
                upsertedCount++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogWarning(ex, "Failed to fetch or parse TraceDeTrail trace {TraceId}", target.TraceId);
            }
        }

        logger.LogInformation("TraceDeTrail: upserted {Count}/{Total} traces in batch", upsertedCount, messages.Length);
    }
}
