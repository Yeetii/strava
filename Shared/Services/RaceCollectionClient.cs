using System.Globalization;
using System.Net;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Shared.Services;

public class RaceCollectionClient(Container container, ILoggerFactory loggerFactory)
    : TiledCollectionClient(
        container,
        loggerFactory,
        FeatureKinds.Race,
        null,
        null,
        storeZoom: DefaultZoom)
{
    public const int DefaultZoom = 8;

    /// <summary>
    /// Parses the numeric slot suffix from a stored race document id
    /// <c>race:{organizerKey}-{n}</c>.
    /// </summary>
    public static bool TryParseRaceDocumentSlotIndex(string documentId, string organizerKey, out int index)
    {
        index = -1;
        var prefix = $"{FeatureKinds.Race}:{organizerKey}-";
        if (!documentId.StartsWith(prefix, StringComparison.Ordinal))
            return false;
        return int.TryParse(documentId.AsSpan(prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out index);
    }

    /// <summary>
    /// Highest slot index from assembled <see cref="StoredFeature.FeatureId"/> values
    /// <c>{organizerKey}-{i}</c>.
    /// </summary>
    public static bool TryGetHighestRaceSlotIndex(string organizerKey, IReadOnlyList<StoredFeature> races, out int maxSlot)
    {
        maxSlot = -1;
        var prefix = $"{organizerKey}-";
        foreach (var r in races)
        {
            if (string.IsNullOrEmpty(r.FeatureId)) continue;
            if (!r.FeatureId.StartsWith(prefix, StringComparison.Ordinal)) continue;
            if (!int.TryParse(r.FeatureId.AsSpan(prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out var idx))
                continue;
            maxSlot = Math.Max(maxSlot, idx);
        }

        return maxSlot >= 0;
    }

    /// <summary>
    /// Sets <c>ttl = 1</c> on race slots whose index is in the range
    /// (<paramref name="highestSlotInUseInclusive"/>, <paramref name="previousMaxSlotInclusive"/>].
    /// Uses cheap point-patches — one per superseded slot (~1 RU each).
    /// When <paramref name="previousMaxSlotInclusive"/> is unknown or not greater than the current
    /// max, nothing is done; superseded slots will be cleaned up on the next run once the
    /// previous max has been recorded.
    /// Returns the document ids that were patched (empty if none).
    /// </summary>
    public async Task<IReadOnlyList<string>> MarkHigherRaceSlotsExpiredAsync(
        string organizerKey,
        int highestSlotInUseInclusive,
        int? previousMaxSlotInclusive = null,
        CancellationToken cancellationToken = default)
    {
        if (!previousMaxSlotInclusive.HasValue || previousMaxSlotInclusive.Value <= highestSlotInUseInclusive)
            return [];

        IReadOnlyList<PatchOperation> ttlPatch = [PatchOperation.Set("/ttl", 1)];
        var expired = new List<string>();
        for (int slot = highestSlotInUseInclusive + 1; slot <= previousMaxSlotInclusive.Value; slot++)
        {
            var docId = $"{FeatureKinds.Race}:{organizerKey}-{slot}";
            try
            {
                await PatchDocument(docId, new PartitionKey(docId), ttlPatch, cancellationToken);
                expired.Add(docId);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Slot was already expired or never created — nothing to do.
            }
        }
        return expired;
    }

    /// <summary>
    /// Sets <c>ttl = 1</c> on specific race documents by their full document IDs.
    /// Used to expire slots that disappeared (e.g. were merged into another slot) between
    /// assembly runs — such slots fall below <c>maxSlot</c> so
    /// <see cref="MarkHigherRaceSlotsExpiredAsync"/> would never reach them.
    /// Returns the document ids that were patched (empty if none).
    /// </summary>
    public async Task<IReadOnlyList<string>> ExpireSpecificSlotsAsync(
        IEnumerable<string> documentIds,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<PatchOperation> ttlPatch = [PatchOperation.Set("/ttl", 1)];
        var expired = new List<string>();
        foreach (var docId in documentIds)
        {
            try
            {
                await PatchDocument(docId, new PartitionKey(docId), ttlPatch, cancellationToken);
                expired.Add(docId);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Already expired or never created — nothing to do.
            }
        }
        return expired;
    }

    /// <summary>
    /// Patches only the <c>/properties</c> path on an existing race document.
    /// Use this when geometry is unchanged but metadata (name, date, url, …) has been updated.
    /// </summary>
    public async Task PatchRacePropertiesAsync(
        string id,
        IDictionary<string, dynamic> properties,
        CancellationToken cancellationToken = default)
    {
        await PatchDocument(
            id,
            new PartitionKey(id),
            [PatchOperation.Set("/properties", properties)],
            cancellationToken);
    }

    public async Task<(int Deleted, string Cutoff)> DeletePastRacesAsync(CancellationToken cancellationToken = default)
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow).ToString("yyyy-MM-dd");
        var queryDefinition = new QueryDefinition(
            "SELECT VALUE c.id FROM c WHERE c.kind = @kind AND IS_DEFINED(c.properties.date) AND c.properties.date < @today")
            .WithParameter("@kind", FeatureKinds.Race)
            .WithParameter("@today", today);

        var ids = await ExecuteQueryAsync<string>(queryDefinition, cancellationToken: cancellationToken);
        var idList = ids.ToList();

        foreach (var id in idList)
        {
            await DeleteDocument(id, new PartitionKey(id), cancellationToken);
        }

        return (idList.Count, today);
    }
}
