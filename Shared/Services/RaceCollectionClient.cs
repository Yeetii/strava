using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;

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
