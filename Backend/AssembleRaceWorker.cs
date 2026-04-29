using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

/// <summary>
/// Triggered by the assembleRace Service Bus queue (enqueued by <see cref="ScrapeRaceWorker"/>
/// after each scrape run). Reads the <see cref="RaceOrganizerDocument"/>, combines discovery
/// hints and scraper output into final <see cref="StoredFeature"/> race documents, and upserts
/// them into the races Cosmos container.
/// </summary>
public partial class AssembleRaceWorker(
    RaceOrganizerClient organizerClient,
    RaceCollectionClient raceCollectionClient,
    ServiceBusClient serviceBusClient,
    ILocationGeocodingService geocodingService,
    ILogger<AssembleRaceWorker> logger)
{
    private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
    private readonly ILocationGeocodingService _geocodingService = geocodingService;

    // Forward version and priority constants from the shared assembler so existing callers compile unchanged.
    internal const int AssemblyVersion = RaceAssembler.AssemblyVersion;
    internal static readonly string[] DiscoveryPriority = RaceAssembler.DiscoveryPriority;
    internal static readonly string[] ScraperPriority = RaceAssembler.ScraperPriority;

    [Function(nameof(AssembleRaceWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.AssembleRace, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var organizerKey = message.Body.ToString().Trim();
        if (string.IsNullOrWhiteSpace(organizerKey))
        {
            logger.LogWarning("Empty organizer key (MessageId={MessageId})", message.MessageId);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "EmptyOrganizerKey", cancellationToken: cancellationToken);
            return;
        }

        var doc = await organizerClient.GetByIdMaybe(
            organizerKey, new PartitionKey(organizerKey), cancellationToken);

        if (doc is null)
        {
            logger.LogWarning("Organizer document not found: {Key}", organizerKey);
            await actions.DeadLetterMessageAsync(message, deadLetterReason: "DocumentNotFound", deadLetterErrorDescription: $"No document for key '{organizerKey}'", cancellationToken: cancellationToken);
            return;
        }

        try
        {
            var races = await RaceAssembler.AssembleRacesAsync(doc, _geocodingService, cancellationToken);
            logger.LogInformation("Built {Count} race feature(s) for {Key}", races.Count, organizerKey);

            // Write each race to Cosmos, skipping or downgrading to a patch when content is unchanged.
            var newHashes = new Dictionary<string, RaceSlotHashes>(races.Count, StringComparer.Ordinal);
            int upserted = 0, patched = 0, skipped = 0;

            foreach (var race in races)
            {
                var slotKey = race.FeatureId!;
                var newHash = ComputeHashes(race);
                newHashes[slotKey] = newHash;

                if (doc.AssemblyHashes?.TryGetValue(slotKey, out var prevHash) == true
                    && prevHash.AssemblyVersion == AssemblyVersion)
                {
                    if (prevHash.GeometryHash == newHash.GeometryHash &&
                        prevHash.PropertiesHash == newHash.PropertiesHash)
                    {
                        skipped++;
                        continue;
                    }

                    if (prevHash.GeometryHash == newHash.GeometryHash)
                    {
                        // Geometry unchanged — patch only the properties path (~1–10 RU vs full upsert).
                        if (await raceCollectionClient.PatchRacePropertiesAsync(race.Id, race.Properties, cancellationToken))
                        {
                            patched++;
                            continue;
                        }

                        await raceCollectionClient.UpsertDocument(race, cancellationToken);
                        upserted++;
                        continue;
                    }
                }

                await raceCollectionClient.UpsertDocument(race, cancellationToken);
                upserted++;
            }

            logger.LogInformation(
                "Assembly writes for {Key}: {Upserted} upserted, {Patched} props-patched, {Skipped} unchanged/skipped",
                organizerKey, upserted, patched, skipped);

            // Expire trailing superseded slots even when all active races were skipped as unchanged.
            // The sweep starts at current max + 1 and stops on the first missing slot.
            RaceCollectionClient.TryGetHighestRaceSlotIndex(organizerKey, races, out var maxSlot);
            var patchOfDeathIds = await raceCollectionClient.MarkHigherRaceSlotsExpiredAsync(
                organizerKey, maxSlot, cancellationToken);

            if (patchOfDeathIds.Count > 0)
            {
                const int maxIdsInLog = 40;
                var idPreview = patchOfDeathIds.Count <= maxIdsInLog
                    ? string.Join(", ", patchOfDeathIds)
                    : string.Join(", ", patchOfDeathIds.Take(maxIdsInLog))
                      + $" … (+{patchOfDeathIds.Count - maxIdsInLog} more)";
                logger.LogInformation(
                    "Patch of death (ttl=1): patched {Count} superseded race document(s) for {Key} (slot index > {MaxSlot}). Ids: {Ids}",
                    patchOfDeathIds.Count, organizerKey, maxSlot, idPreview);
            }

            // Expire slots that disappeared from this assembly run (e.g. a Point merged into a
            // LineString shifts or removes a slot below maxSlot, which the range-based expiry
            // above would never reach).
            // Two complementary detection strategies:
            //   1. Diff against previous AssemblyHashes (covers steady-state case).
            //   2. Sweep [0, maxSlot) for holes (covers old organizers with null AssemblyHashes
            //      and any transition where hashes were stored without a now-merged slot).
            var newSlotKeys = newHashes.Keys.ToHashSet(StringComparer.Ordinal);
            var mergedAwayDocIds = new HashSet<string>(StringComparer.Ordinal);

            foreach (var k in (doc.AssemblyHashes ?? []).Keys)
                if (!newSlotKeys.Contains(k))
                    mergedAwayDocIds.Add($"{FeatureKinds.Race}:{k}");

            for (int slot = 0; slot < maxSlot; slot++)
            {
                var slotKey = $"{organizerKey}-{slot}";
                if (!newSlotKeys.Contains(slotKey))
                    mergedAwayDocIds.Add($"{FeatureKinds.Race}:{slotKey}");
            }

            var mergedAwayIds = mergedAwayDocIds.ToList();
            if (mergedAwayIds.Count > 0)
            {
                var mergedPatched = await raceCollectionClient.ExpireSpecificSlotsAsync(mergedAwayIds, cancellationToken);
                if (mergedPatched.Count > 0)
                    logger.LogInformation(
                        "Patch of death (merged slots): expired {Count} merged-away race document(s) for {Key}. Ids: {Ids}",
                        mergedPatched.Count, organizerKey, string.Join(", ", mergedPatched));
            }

            // Record assembly metadata (timestamp, max slot, per-slot hashes) on the organizer document.
            await organizerClient.PatchLastAssembledAsync(
                organizerKey,
                maxSlot >= 0 ? maxSlot : null,
                newHashes,
                cancellationToken);
            await TryCompleteAsync(actions, message, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                ex, actions, message, _serviceBusClient, ServiceBusConfig.AssembleRace, logger, cancellationToken);
        }
    }

    // ── Settlement helpers ───────────────────────────────────────────────

    private async Task TryCompleteAsync(ServiceBusMessageActions actions, ServiceBusReceivedMessage message, CancellationToken ct)
    {
        try { await actions.CompleteMessageAsync(message, ct); }
        catch (Exception ex) { logger.LogDebug(ex, "Could not complete message (manual trigger?)"); }
    }

    private async Task TryDeadLetterAsync(ServiceBusMessageActions actions, ServiceBusReceivedMessage message, string key, Exception inner)
    {
        try
        {
            await actions.DeadLetterMessageAsync(message,
                deadLetterReason: nameof(AssembleRaceWorker),
                deadLetterErrorDescription: $"{key}: {inner.Message}");
        }
        catch (Exception ex) { logger.LogDebug(ex, "Could not dead-letter message (manual trigger?)"); }
    }

    // ── Assembly — forwarding to RaceAssembler ────────────────────────────

    public static List<StoredFeature> AssembleRaces(RaceOrganizerDocument doc)
        => RaceAssembler.AssembleRaces(doc);

    public static Task<List<StoredFeature>> AssembleRacesAsync(
        RaceOrganizerDocument doc,
        ILocationGeocodingService? geocodingService,
        CancellationToken cancellationToken)
        => RaceAssembler.AssembleRacesAsync(doc, geocodingService, cancellationToken);

    // ── Merge helpers — forwarding to RaceAssembler ───────────────────────

    public static SourceDiscovery MergeDiscovery(Dictionary<string, List<SourceDiscovery>>? discovery)
        => RaceAssembler.MergeDiscovery(discovery);

    internal static List<(string Source, SourceDiscovery Entry)> FlattenDiscoveries(
        Dictionary<string, List<SourceDiscovery>>? discovery)
        => RaceAssembler.FlattenDiscoveries(discovery);

    internal static List<(string Source, SourceDiscovery Entry)> DeduplicateDiscoveriesByDistance(
        IReadOnlyList<(string Source, SourceDiscovery Entry)> discoveries)
        => RaceAssembler.DeduplicateDiscoveriesByDistance(discoveries);

    // ── Property building — forwarding to RaceAssembler ──────────────────

    public static Dictionary<string, dynamic> BuildProperties(
        SourceDiscovery discovery,
        string? scraperKey,
        ScrapedRouteOutput? route,
        string? scraperImageUrl,
        string? scraperLogoUrl,
        string websiteUrl)
        => RaceAssembler.BuildProperties(discovery, scraperKey, route, scraperImageUrl, scraperLogoUrl, websiteUrl);

    public static string? PickBestDate(string? scraperKey, string? routeDate, string? discoveryDate)
        => RaceAssembler.PickBestDate(scraperKey, routeDate, discoveryDate);

    internal static string SanitizeName(string name, string? effectiveDistance)
        => RaceAssembler.SanitizeName(name, effectiveDistance);

    // ── Route collection — forwarding to RaceAssembler ───────────────────

    public static List<(string ScraperKey, ScrapedRouteOutput Route)> CollectRoutes(
        Dictionary<string, ScraperOutput>? scrapers)
        => RaceAssembler.CollectRoutes(scrapers);

    internal static SourceDiscovery FindDiscoveryForLabel(
        string label,
        IReadOnlyList<(string Source, SourceDiscovery Entry)> flatDiscoveries,
        SourceDiscovery fallback)
        => RaceAssembler.FindDiscoveryForLabel(label, flatDiscoveries, fallback);

    // ── Content hashing — forwarding to RaceAssembler ────────────────────

    internal static RaceSlotHashes ComputeHashes(StoredFeature race)
        => RaceAssembler.ComputeHashes(race);
}
