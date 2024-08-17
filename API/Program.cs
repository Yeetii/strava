using System.Configuration;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker.Extensions.OpenApi.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
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
        services.AddSingleton(new SocketsHttpHandler());
        services.AddHttpClient();
        services.AddSingleton(serviceProvider =>
        {
            SocketsHttpHandler socketsHttpHandler = serviceProvider.GetRequiredService<SocketsHttpHandler>();
            CosmosClientOptions cosmosClientOptions = new()
            {
                HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false)
            };

            string cosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection") ?? throw new ConfigurationErrorsException("No cosmos connection string found");
            return new CosmosClient(cosmosDbConnectionString, cosmosClientOptions);
        });
        // TODO: Setup collection clients via factory/builder
        services.AddSingleton(ServiceProvider => 
        {
            var databaseName = configuration.GetValue<string>("OsmDb") ?? throw new ConfigurationErrorsException("No database name found");
            var containerName = configuration.GetValue<string>("PeaksContainer") ?? throw new ConfigurationErrorsException("No peaks container name found");
            var cosmos = ServiceProvider.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            return new CollectionClient<StoredFeature>(container);
        });
        services.AddSingleton(ServiceProvider => 
        {
            var databaseName = configuration.GetValue<string>("CosmosDb") ?? throw new ConfigurationErrorsException("No database name found");
            var containerName = configuration.GetValue<string>("SummitedPeaksContainer") ?? throw new ConfigurationErrorsException("No summited peaks container name found");
            var cosmos = ServiceProvider.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            return new CollectionClient<SummitedPeak>(container);
        });
        services.AddSingleton(ServiceProvider => 
        {
            var databaseName = configuration.GetValue<string>("CosmosDb") ?? throw new ConfigurationErrorsException("No database name found");
            var containerName = configuration.GetValue<string>("UsersContainer") ?? throw new ConfigurationErrorsException("No user container name found");
            var cosmos = ServiceProvider.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            return new CollectionClient<Shared.Models.User>(container);
        });
        services.AddScoped(ServiceProvider => {
            var httpClient = ServiceProvider.GetRequiredService<HttpClient>();
            return new AuthenticationApi(httpClient, configuration);
        });
    })
    .ConfigureOpenApi()
    .Build();

host.Run();
