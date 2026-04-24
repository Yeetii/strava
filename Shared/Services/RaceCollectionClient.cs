using System.Globalization;
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
    /// Queries race document ids under <c>race:{organizerKey}-*</c> and sets <c>ttl = 1</c> on any
    /// whose slot index is greater than <paramref name="highestSlotInUseInclusive"/> (Cosmos TTL in seconds).
    /// Returns the document ids that were patched (empty if none).
    /// </summary>
    public async Task<IReadOnlyList<string>> MarkHigherRaceSlotsExpiredAsync(
        string organizerKey,
        int highestSlotInUseInclusive,
        CancellationToken cancellationToken = default)
    {
        var idPrefix = $"{FeatureKinds.Race}:{organizerKey}-";
        var query = new QueryDefinition(
                "SELECT VALUE c.id FROM c WHERE STARTSWITH(c.id, @prefix) AND c.kind = @kind")
            .WithParameter("@prefix", idPrefix)
            .WithParameter("@kind", FeatureKinds.Race);

        var ids = (await ExecuteQueryAsync<string>(query, cancellationToken: cancellationToken)).ToList();
        var toExpire = new List<string>();
        foreach (var id in ids)
        {
            if (!TryParseRaceDocumentSlotIndex(id, organizerKey, out var slot)) continue;
            if (slot > highestSlotInUseInclusive)
                toExpire.Add(id);
        }

        if (toExpire.Count == 0) return [];

        IReadOnlyList<PatchOperation> ttlPatch = [PatchOperation.Set("/ttl", 1)];
        var patches = toExpire.Select(id => (id, new PartitionKey(id), ttlPatch)).ToList();
        await PatchDocuments(patches, cancellationToken);
        return toExpire;
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
