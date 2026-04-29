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

    public sealed record RaceTtlStatus(string Id, string? FeatureId, int? Ttl, string? Date);
    private sealed record RacePatchTarget(string Id, int X, int Y);

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
    /// Sets <c>ttl = 1</c> on trailing race slots starting immediately after the current highest
    /// assembled slot and keeps scanning upward until Cosmos returns 404.
    /// Uses cheap point-patches — one per superseded slot (~1 RU each).
    /// Returns the document ids that were patched (empty if none).
    /// </summary>
    public async Task<IReadOnlyList<string>> MarkHigherRaceSlotsExpiredAsync(
        string organizerKey,
        int highestSlotInUseInclusive,
        CancellationToken cancellationToken = default)
    {
        IReadOnlyList<PatchOperation> ttlPatch = [PatchOperation.Set("/ttl", 1)];
        var expired = new List<string>();
        for (int slot = highestSlotInUseInclusive + 1; ; slot++)
        {
            var docId = $"{FeatureKinds.Race}:{organizerKey}-{slot}";
            if (!await TryPatchRaceDocumentByIdAsync(docId, ttlPatch, cancellationToken))
                break;

            expired.Add(docId);
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
            if (await TryPatchRaceDocumentByIdAsync(docId, ttlPatch, cancellationToken))
                expired.Add(docId);
        }
        return expired;
    }

    /// <summary>
    /// Patches only the <c>/properties</c> path on an existing race document.
    /// Use this when geometry is unchanged but metadata (name, date, url, …) has been updated.
    /// </summary>
    public async Task<bool> PatchRacePropertiesAsync(
        string id,
        IDictionary<string, dynamic> properties,
        CancellationToken cancellationToken = default)
    {
        return await TryPatchRaceDocumentByIdAsync(
            id,
            [PatchOperation.Set("/properties", properties)],
            cancellationToken);
    }

    public async Task<(int Expired, string Cutoff)> ExpirePastRacesAsync(CancellationToken cancellationToken = default)
    {
        var today = DateOnly.FromDateTime(DateTime.UtcNow).ToString("yyyy-MM-dd");
        var queryDefinition = new QueryDefinition(
            "SELECT VALUE c.id FROM c WHERE c.kind = @kind AND IS_DEFINED(c.properties.date) AND c.properties.date < @today AND (NOT IS_DEFINED(c.ttl) OR c.ttl != 1)")
            .WithParameter("@kind", FeatureKinds.Race)
            .WithParameter("@today", today);

        var ids = await ExecuteQueryAsync<string>(queryDefinition, cancellationToken: cancellationToken);
        var idList = ids.ToList();

        IReadOnlyList<PatchOperation> ttlPatch = [PatchOperation.Set("/ttl", 1)];

        foreach (var id in idList)
        {
            await TryPatchRaceDocumentByIdAsync(id, ttlPatch, cancellationToken);
        }

        return (idList.Count, today);
    }

    public async Task<IReadOnlyList<RaceTtlStatus>> GetRaceTtlStatusAsync(
        string organizerKey,
        CancellationToken cancellationToken = default)
    {
        var prefix = $"{FeatureKinds.Race}:{organizerKey}-";
        var queryDefinition = new QueryDefinition(
            @"SELECT c.id, c.featureId, c.ttl, c.properties.date
              FROM c
              WHERE STARTSWITH(c.id, @prefix)")
            .WithParameter("@prefix", prefix);

        var items = (await ExecuteQueryAsync<RaceTtlStatus>(queryDefinition, cancellationToken: cancellationToken))
            .OrderBy(item => TryParseRaceDocumentSlotIndex(item.Id, organizerKey, out var index) ? index : int.MaxValue)
            .ThenBy(item => item.Id, StringComparer.Ordinal)
            .ToList();

        return items;
    }

    private async Task<bool> TryPatchRaceDocumentByIdAsync(
        string id,
        IReadOnlyList<PatchOperation> operations,
        CancellationToken cancellationToken)
    {
        var target = await GetRacePatchTargetAsync(id, cancellationToken);
        if (target is null)
            return false;

        try
        {
            await PatchDocument(id, BuildRacePartitionKey(target.X, target.Y), operations, cancellationToken);
            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private async Task<RacePatchTarget?> GetRacePatchTargetAsync(string id, CancellationToken cancellationToken)
    {
        var queryDefinition = new QueryDefinition(
            "SELECT c.id, c.x, c.y FROM c WHERE c.id = @id")
            .WithParameter("@id", id);

        return (await ExecuteQueryAsync<RacePatchTarget>(queryDefinition, cancellationToken: cancellationToken))
            .FirstOrDefault();
    }

    private static PartitionKey BuildRacePartitionKey(int x, int y)
        => new PartitionKeyBuilder().Add((double)x).Add((double)y).Build();
}
