using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Shared.Services;

public class CollectionClientBuilder(IServiceCollection services)
{
    public CollectionClientBuilder AddCollection<T>(string databaseName, string containerName) where T : IDocument
    {
        services.AddSingleton(serviceProvider =>
        {
            var cosmosClient = serviceProvider.GetRequiredService<CosmosClient>();
            var container = cosmosClient.GetContainer(databaseName, containerName);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            return new CollectionClient<T>(container, loggerFactory);
        });
        return this;
    }

    /// <summary>
    /// Registers all Overpass-backed feature caches against a single shared container, keyed by
    /// <see cref="FeatureKinds"/>. Consumers inject with <c>[FromKeyedServices(FeatureKinds.X)] TiledCollectionClient</c>.
    /// The container must have <c>DefaultTimeToLive = 2592000</c> (30d) configured in Cosmos.
    /// </summary>
    public CollectionClientBuilder AddOsmFeatureCaches(string databaseName, string containerName)
    {
        AddKeyedTiledClient(databaseName, containerName, FeatureKinds.Peak, op => op.GetPeaks, op => op.GetPeaksByIds);
        AddKeyedTiledClient(databaseName, containerName, FeatureKinds.Path, op => op.GetPaths);
        AddKeyedTiledClient(databaseName, containerName, FeatureKinds.ProtectedArea, op => op.GetProtectedAreas);

        services.AddSingleton(sp =>
        {
            var cosmos = sp.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var overpass = sp.GetRequiredService<OverpassClient>();
            return new AdminBoundariesCollectionClient(container, loggerFactory, overpass);
        });
        return this;
    }

    private void AddKeyedTiledClient(
        string databaseName,
        string containerName,
        string kind,
        Func<OverpassClient, Func<Coordinate, Coordinate, CancellationToken, Task<IEnumerable<BAMCIS.GeoJSON.Feature>>>> fetcherSelector,
        Func<OverpassClient, Func<IEnumerable<string>, CancellationToken, Task<IEnumerable<BAMCIS.GeoJSON.Feature>>>>? fetchByIdsSelector = null)
    {
        services.AddKeyedSingleton(kind, (sp, _) =>
        {
            var cosmos = sp.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var overpass = sp.GetRequiredService<OverpassClient>();
            return new TiledCollectionClient(
                container,
                loggerFactory,
                kind,
                fetcherSelector(overpass),
                fetchByIdsSelector?.Invoke(overpass));
        });
    }
}
