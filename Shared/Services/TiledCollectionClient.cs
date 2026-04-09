using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Shared.Services;

public abstract class TiledCollectionClient(Container container, ILoggerFactory loggerFactory, OverpassClient overpassClient)
    : CollectionClient<StoredFeature>(container, loggerFactory)
{
    protected readonly OverpassClient _overpassClient = overpassClient;

    protected async Task<IEnumerable<StoredFeature>> QueryByListOfKeys(IEnumerable<(int x, int y)> keys, int zoom)
    {
        var keyConditions = string.Join(" OR ", keys.Select((key, i) => $"(c.x = @x{i} AND c.y = @y{i})"));
        var queryDefinition = new QueryDefinition($"SELECT * FROM c WHERE ({keyConditions}) AND c.zoom = @zoom")
            .WithParameter("@zoom", zoom);
        int index = 0;
        foreach (var (x, y) in keys)
        {
            queryDefinition = queryDefinition
                .WithParameter($"@x{index}", x)
                .WithParameter($"@y{index}", y);
            index++;
        }
        return await ExecuteQueryAsync<StoredFeature>(queryDefinition);
    }

    protected static IEnumerable<(int x, int y)> GetMissingTiles(IEnumerable<StoredFeature> documents, IEnumerable<(int x, int y)> keys)
    {
        var keysInDocuments = new HashSet<(int x, int y)>(documents.Select(p => (p.X, p.Y)));
        return keys.Where(p => !keysInDocuments.Contains((p.x, p.y)));
    }

    protected abstract Task<IEnumerable<StoredFeature>> FetchMissingTile(int x, int y, int zoom);

    public async Task<IEnumerable<StoredFeature>> FetchByTiles(IEnumerable<(int x, int y)> keys, int zoom = 11)
    {
        if (!keys.Any())
            return [];

        var docs = (await QueryByListOfKeys(keys, zoom)).ToList();
        var missingTiles = GetMissingTiles(docs, keys);
        foreach (var (x, y) in missingTiles)
            docs.AddRange(await FetchMissingTile(x, y, zoom));

        return docs.Where(d => !d.Id.StartsWith("empty-"));
    }
}
