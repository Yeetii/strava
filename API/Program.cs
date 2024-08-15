using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker.Extensions.OpenApi.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Models;
using Shared.Services;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureAppConfiguration((hostingContext, config) => 
    {
        config.AddEnvironmentVariables();
    })
    .ConfigureServices((hostingContext, services) =>{
        var configuration = hostingContext.Configuration;

        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        });
        services.AddHttpClient();
        services.AddSingleton(new SocketsHttpHandler());
        services.AddSingleton(serviceProvider =>
        {
            SocketsHttpHandler socketsHttpHandler = serviceProvider.GetRequiredService<SocketsHttpHandler>();
            CosmosClientOptions cosmosClientOptions = new()
            {
                HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false)
            };

            string cosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection") ?? throw new Exception("No cosmos connection string found");
            return new CosmosClient(cosmosDbConnectionString, cosmosClientOptions);
        });
        services.AddSingleton(ServiceProvider => 
        {
            var databaseName = configuration.GetValue<string>("OsmDb") ?? throw new Exception("No database name found");
            var containerName = configuration.GetValue<string>("PeaksContainer") ?? throw new Exception("No peaks container name found");
            var cosmos = ServiceProvider.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            return new CollectionClient<StoredFeature>(container);
        });
    })
    .ConfigureOpenApi()
    .Build();

host.Run();
