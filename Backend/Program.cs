using System.Configuration;
using Microsoft.Extensions.Configuration;
using System.Net.Http.Headers;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using Shared.Constants;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Backend;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((hostingContext, services) =>
    {
        var configuration = hostingContext.Configuration;
        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
            options.Converters.Add(new GeometrySystemTextJsonConverter());
            options.Converters.Add(new FeatureIdJsonConverter());
        });
        var socketsHttpHandler = new SocketsHttpHandler();
        services.AddSingleton(socketsHttpHandler);
        services.AddHttpClient(
            "apiClient",
            client =>
            {
                client.BaseAddress = new Uri(configuration.GetValue<string>("ApiUrl") ?? throw new ConfigurationErrorsException("No API Url found in config"));
            });
        services.AddHttpClient(
            "backendApiClient",
            client =>
            {
                client.BaseAddress = new Uri(configuration.GetValue<string>("BackendApiUrl") ?? throw new ConfigurationErrorsException("No Backend API Url found in config"));
            });
        services.AddHttpClient(
            "stravaClient",
            client =>
            {
                client.BaseAddress = new Uri("https://www.strava.com/api/v3/");
            });
        services.AddSingleton(serviceProvider =>
        {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var stravaClient = httpClientFactory.CreateClient("stravaClient");
            return new ActivitiesApi(stravaClient);
        });
        services.AddHttpClient<OverpassClient>(
            client =>
            {
                client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
                client.Timeout = TimeSpan.FromMinutes(10); // large country geometries (Russia, Canada) can take several minutes
            });
        services.AddHttpClient<ILocationGeocodingService, NominatimLocationGeocodingService>(
            client =>
            {
                client.BaseAddress = new Uri("https://nominatim.openstreetmap.org/");
                client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
                client.Timeout = TimeSpan.FromSeconds(10);
            });
        string cosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection") ?? throw new Exception("No cosmos connection string found");
        CosmosClientOptions cosmosClientOptions = new()
        {
            HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false),
            SerializerOptions = new CosmosSerializationOptions { PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase },
            AllowBulkExecution = true,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes(3)
        };
        // Register as instance so the DI container does not own or dispose it during host shutdown.
        // Using a factory (AddSingleton(factory)) would cause the container to call Dispose() when
        // the host recycles, failing any in-flight Cosmos operations with ObjectDisposedException.
        services.AddSingleton(new CosmosClient(cosmosDbConnectionString, cosmosClientOptions));

        services.AddSingleton(serviceProvider =>
        {
            var configuration = serviceProvider.GetRequiredService<IConfiguration>();
            var storageConnectionString = configuration.GetValue<string>("AzureWebJobsStorage")
                ?? throw new Exception("No AzureWebJobsStorage connection string found");
            return new BlobServiceClient(storageConnectionString);
        });

        new CollectionClientBuilder(services)
            .AddCollection<SummitedPeak>(DatabaseConfig.CosmosDb, DatabaseConfig.SummitedPeaksContainer)
            .AddCollection<Shared.Models.User>(DatabaseConfig.CosmosDb, DatabaseConfig.UsersContainer)
            .AddCollection<Session>(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer)
            .AddCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer)
            .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
            .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer)
            .AddOsmFeatureCaches(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        services.AddSingleton(serviceProvider =>
        {
            var cosmosClient = serviceProvider.GetRequiredService<CosmosClient>();
            var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.RacesContainer);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            return new RaceCollectionClient(container, loggerFactory);
        });

        services.AddSingleton(serviceProvider =>
        {
            var cfg = serviceProvider.GetRequiredService<IConfiguration>();
            var connStr = cfg.GetValue<string>("BlobStorageConnection")
                ?? throw new Exception("BlobStorageConnection is not configured");
            var containerName = BlobContainerNames.RaceOrganizers;
            var containerClient = new BlobContainerClient(connStr, containerName);
            containerClient.CreateIfNotExists();
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            return new BlobOrganizerStore(containerClient, loggerFactory);
        });

        services.AddSingleton<AdminBoundaryMetricsEnricher>();

        services.AddScoped(serviceProvider =>
        {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var httpClient = httpClientFactory.CreateClient();
            return new AuthenticationApi(httpClient, configuration);
        });
        services.AddSingleton(s =>
        {
            var sbConnectionString = configuration.GetValue<string>("ServicebusConnection");
            return new ServiceBusClient(sbConnectionString);
        });
        services.AddScoped(serviceProvider =>
        {
            var usersCollection = serviceProvider.GetRequiredService<CollectionClient<Shared.Models.User>>();
            var sessionsCollection = serviceProvider.GetRequiredService<CollectionClient<Session>>();
            return new UserAuthenticationService(usersCollection, sessionsCollection);
        });
        services.AddScoped<ISummitsCalculator>(serviceProvider =>
        {
            return new SummitsCalculatorWithBoundingBoxFilter();
        });
        services.AddSingleton<RaceDiscoveryService>();
        services.AddSingleton<DiscoverDuvRaces>();
        services.AddSingleton<DiscoverTraceDeTrailRaces>();
        services.AddSingleton<DiscoverItraRaces>();
        services.AddSingleton<DiscoverTrailrunningSwedenRaces>();
        services.AddSingleton<DiscoverSkyrunningRaces>();
        services.AddSingleton<DiscoverLopplistanRaces>();
    })
    .Build();

host.Run();
