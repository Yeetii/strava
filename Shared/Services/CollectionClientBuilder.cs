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

    public CollectionClientBuilder AddPeaksCollection(string databaseName, string containerName)
    {
        services.AddSingleton(serviceProvider =>
        {
            var cosmosClient = serviceProvider.GetRequiredService<CosmosClient>();
            var container = cosmosClient.GetContainer(databaseName, containerName);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            var overpassClient = serviceProvider.GetRequiredService<OverpassClient>();
            return new PeaksCollectionClient(container, loggerFactory, overpassClient);
        });
        return this;
    }

    public CollectionClientBuilder AddPathsCollection(string databaseName, string containerName)
    {
        services.AddSingleton(serviceProvider =>
        {
            var cosmosClient = serviceProvider.GetRequiredService<CosmosClient>();
            var container = cosmosClient.GetContainer(databaseName, containerName);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            var overpassClient = serviceProvider.GetRequiredService<OverpassClient>();
            return new PathsCollectionClient(container, loggerFactory, overpassClient);
        });
        return this;
    }
}

