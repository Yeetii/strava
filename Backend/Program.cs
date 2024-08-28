using System.Configuration;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((hostingContext, services) =>{
        var configuration = hostingContext.Configuration;
        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        });
        services.AddSingleton(new SocketsHttpHandler());
        services.AddHttpClient(
            "backendApiClient",
            client =>
            {
                client.BaseAddress = new Uri(configuration.GetValue<string>("BackendApiUrl") ?? throw new ConfigurationErrorsException("No API Url found in config"));
            });
        services.AddHttpClient(
            "stravaClient",
            client =>
            {
                client.BaseAddress = new Uri("https://www.strava.com/api/v3/");
            });
        services.AddSingleton(serviceProvider => {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var stravaClient = httpClientFactory.CreateClient("stravaClient");
            return new ActivitiesApi(stravaClient);
        });
        services.AddSingleton(serviceProvider =>
        {
            SocketsHttpHandler socketsHttpHandler = serviceProvider.GetRequiredService<SocketsHttpHandler>();
            CosmosClientOptions cosmosClientOptions = new()
            {
                HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false),
                SerializerOptions = new CosmosSerializationOptions{PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase},
                AllowBulkExecution = true,

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
        services.AddSingleton(ServiceProvider => 
        {
            var databaseName = configuration.GetValue<string>("CosmosDb") ?? throw new Exception("No database name found");
            var containerName = configuration.GetValue<string>("ActivitiesContainer") ?? throw new Exception("No peaks container name found");
            var cosmos = ServiceProvider.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            return new CollectionClient<Activity>(container);
        });
        services.AddSingleton(ServiceProvider => 
        {
            var databaseName = configuration.GetValue<string>("CosmosDb") ?? throw new Exception("No database name found");
            var containerName = configuration.GetValue<string>("SummitedPeaksContainer") ?? throw new Exception("No summited peaks container name found");
            var cosmos = ServiceProvider.GetRequiredService<CosmosClient>();
            var container = cosmos.GetContainer(databaseName, containerName);
            return new CollectionClient<SummitedPeak>(container);
        });
        services.AddScoped(serviceProvider => {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var httpClient = httpClientFactory.CreateClient();
            return new AuthenticationApi(httpClient, configuration);
        });
    })
    .Build();

host.Run();
