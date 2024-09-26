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
}

