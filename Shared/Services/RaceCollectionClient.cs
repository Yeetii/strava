using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Shared.Services;

public class RaceCollectionClient(Container container, ILoggerFactory loggerFactory)
    : CollectionClient<StoredFeature>(container, loggerFactory)
{
    public const int DefaultZoom = 8;

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

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(
        IEnumerable<(int x, int y)> keys,
        int zoom = DefaultZoom,
        CancellationToken cancellationToken = default)
    {
        if (!keys.Any())
            return [];

        var keyConditions = string.Join(" OR ", keys.Select((_, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryDefinition = new QueryDefinition($"SELECT * FROM c WHERE ({keyConditions}) AND c.zoom = @zoom AND c.kind = @kind")
            .WithParameter("@zoom", zoom)
            .WithParameter("@kind", FeatureKinds.Race);

        int index = 0;
        foreach (var (x, y) in keys)
        {
            queryDefinition = queryDefinition
                .WithParameter($"@x{index}", x)
                .WithParameter($"@y{index}", y);
            index++;
        }

        var documents = await ExecuteQueryAsync<StoredFeature>(queryDefinition, cancellationToken: cancellationToken);
        return documents
            .Where(d => !d.Id.StartsWith("empty-", StringComparison.Ordinal))
            .DistinctBy(d => d.LogicalId);
    }
}
