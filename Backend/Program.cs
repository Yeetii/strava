using System.Configuration;
using Microsoft.Extensions.Configuration;
using System.Net.Http.Headers;
using System.Text.Json;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Monitor.Query;
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
using Shared.Services.Shards;

var host = new HostBuilder()
    .ConfigureAppConfiguration(c => c.AddJsonFile("local.settings.shared.json", optional: true))
    .ConfigureFunctionsWebApplication()
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
            .AddCollection<UserSyncItem>(DatabaseConfig.CosmosDb, DatabaseConfig.UserSyncItemsContainer)
            .AddCollection<Session>(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer)
            .AddCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer)
            .AddTiledCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer, storeZoom: configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12)
            .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
            .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer)
            .AddOsmFeatureCaches(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

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
        services.AddSingleton(serviceProvider =>
        {
            var cfg = serviceProvider.GetRequiredService<IConfiguration>();
            var connStr = cfg.GetValue<string>("BlobStorageConnection")
                ?? throw new Exception("BlobStorageConnection is not configured");
            var containerClient = new BlobContainerClient(connStr, BlobContainerNames.HighwaysShards);
            containerClient.CreateIfNotExists();
            return new HighwaysShardContainer(containerClient);
        });
        services.AddSingleton<IShardRepository>(serviceProvider =>
        {
            var repositoryLogger = serviceProvider.GetRequiredService<ILogger<BlobShardRepository>>();
            var containerClient = serviceProvider.GetRequiredService<HighwaysShardContainer>().Client;
            var overpass = serviceProvider.GetRequiredService<OverpassClient>();
            var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
            var shardBufferMeters = configuration.GetValue<int?>(AppConfig.BlobShardBufferMeters) ?? 200;
            return new BlobShardRepository(containerClient, repositoryLogger, overpass.GetHighways, shardZoom, shardBufferMeters);
        });
        services.AddSingleton(serviceProvider =>
        {
            var shardRepository = serviceProvider.GetRequiredService<IShardRepository>();
            var shardLogger = serviceProvider.GetRequiredService<ILogger<ShardFeatureClient>>();
            var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
            return new ShardFeatureClient(shardRepository, shardLogger, shardZoom);
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
            var sbConnectionString = configuration.GetValue<string>("ServiceBusConnection");
            return new ServiceBusClient(sbConnectionString);
        });
        services.AddSingleton(s =>
        {
            var sbConnectionString = configuration.GetValue<string>("ServiceBusConnection");
            return new ServiceBusAdministrationClient(sbConnectionString);
        });
        services.AddScoped(serviceProvider =>
        {
            var usersCollection = serviceProvider.GetRequiredService<CollectionClient<Shared.Models.User>>();
            var sessionsCollection = serviceProvider.GetRequiredService<CollectionClient<Session>>();
            return new UserAuthenticationService(usersCollection, sessionsCollection);
        });
        services.AddScoped<UserSyncStatusService>();
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

        var cosmosResourceId = configuration.GetValue<string>("CosmosAccountResourceId");
        if (!string.IsNullOrEmpty(cosmosResourceId))
            services.AddSingleton(new MetricsQueryClient(new DefaultAzureCredential()));
    })
    .Build();

ServiceBusRescheduler.Initialize(
    host.Services.GetRequiredService<ServiceBusAdministrationClient>(),
    metricsQueryClient: host.Services.GetService<MetricsQueryClient>(),
    cosmosResourceId: host.Services.GetRequiredService<IConfiguration>().GetValue<string>("CosmosAccountResourceId"));

host.Run();
