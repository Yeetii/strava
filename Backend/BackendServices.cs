using System.Configuration;
using System.Net.Http.Headers;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;
using Shared.Services.Shards;
using Shared.Services.StravaClient;
using Shared.Hosting;

namespace Backend;

/// <summary>
/// The Backend DI graph used by the worker-schedule and worker-heavy hosts.
/// The merged `api` host registers the few Backend-only services its HTTP
/// functions need directly; see API/Program.cs.
/// </summary>
public static class BackendServices
{
    public static void AddBackendServices(
        this IServiceCollection services,
        IConfiguration configuration,
        IServiceCollection? _ = null)
    {
        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
            options.Converters.Add(new GeometrySystemTextJsonConverter());
            options.Converters.Add(new FeatureIdJsonConverter());
        });

        var socketsHttpHandler = new SocketsHttpHandler();
        services.AddSingleton(socketsHttpHandler);
        services.AddHttpClient("apiClient", client =>
            client.BaseAddress = new Uri(configuration.GetValue<string>("ApiUrl")
                ?? throw new ConfigurationErrorsException("No API Url found in config")));
        services.AddHttpClient("backendApiClient", client =>
            client.BaseAddress = new Uri(configuration.GetValue<string>("BackendApiUrl")
                ?? throw new ConfigurationErrorsException("No Backend API Url found in config")));
        services.AddHttpClient("stravaClient", client =>
            client.BaseAddress = new Uri("https://www.strava.com/api/v3/"));
        services.AddSingleton(serviceProvider =>
        {
            var factory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            return new ActivitiesApi(factory.CreateClient("stravaClient"));
        });
        services.AddHttpClient<OverpassClient>(client =>
        {
            client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
            client.Timeout = TimeSpan.FromMinutes(10);
        });
        services.AddHttpClient<ILocationGeocodingService, NominatimLocationGeocodingService>(client =>
        {
            client.BaseAddress = new Uri("https://nominatim.openstreetmap.org/");
            client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
            client.Timeout = TimeSpan.FromSeconds(10);
        });
        services.AddSharedGeocodingCache(configuration);

        string cosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection")
            ?? throw new Exception("No cosmos connection string found");
        CosmosClientOptions cosmosClientOptions = new()
        {
            HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false),
            SerializerOptions = new CosmosSerializationOptions { PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase },
            AllowBulkExecution = true,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes(3)
        };
        services.AddSingleton(new CosmosClient(cosmosDbConnectionString, cosmosClientOptions));

        services.AddSingleton(serviceProvider =>
        {
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
            .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
            .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer)
            .AddOsmFeatureCaches(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        services.AddSingleton(serviceProvider =>
        {
            var connStr = configuration.GetValue<string>("BlobStorageConnection")
                ?? throw new Exception("BlobStorageConnection is not configured");
            var containerClient = new BlobContainerClient(connStr, BlobContainerNames.RaceOrganizers);
            containerClient.CreateIfNotExists();
            return new BlobOrganizerStore(containerClient,
                serviceProvider.GetRequiredService<ILoggerFactory>());
        });
        services.AddSingleton(serviceProvider =>
        {
            var connStr = configuration.GetValue<string>("BlobStorageConnection")
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
            return new BlobShardRepository(
                containerClient, repositoryLogger, overpass.GetHighways, shardZoom, shardBufferMeters,
                async (x, y, shard, cancellationToken) =>
                {
                    var zoomIndexService = serviceProvider.GetRequiredService<HighwayZoomIndexService>();
                    await zoomIndexService.SyncIndexesForShardAsync(x, y, shard, cancellationToken);
                });
        });
        services.AddSingleton(serviceProvider =>
        {
            var shardRepository = serviceProvider.GetRequiredService<IShardRepository>();
            var shardLogger = serviceProvider.GetRequiredService<ILogger<ShardFeatureClient>>();
            var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
            return new ShardFeatureClient(shardRepository, shardLogger, shardZoom);
        });
        services.AddSingleton(serviceProvider =>
        {
            var containerClient = serviceProvider.GetRequiredService<HighwaysShardContainer>().Client;
            var shardRepository = serviceProvider.GetRequiredService<IShardRepository>();
            var indexLogger = serviceProvider.GetRequiredService<ILogger<HighwayZoomIndexService>>();
            var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
            return new HighwayZoomIndexService(containerClient, shardRepository, indexLogger, shardZoom);
        });

        services.AddSingleton<AdminBoundaryMetricsEnricher>();

        services.AddScoped(serviceProvider =>
        {
            var factory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            return new AuthenticationApi(factory.CreateClient(), configuration);
        });
        services.AddSingleton(_ => new ServiceBusClient(
            configuration.GetValue<string>("ServiceBusConnection")));
        services.AddSingleton(_ => new ServiceBusAdministrationClient(
            configuration.GetValue<string>("ServiceBusConnection")));
        services.AddScoped(serviceProvider =>
        {
            var usersCollection = serviceProvider.GetRequiredService<CollectionClient<Shared.Models.User>>();
            var sessionsCollection = serviceProvider.GetRequiredService<CollectionClient<Session>>();
            return new UserAuthenticationService(usersCollection, sessionsCollection);
        });
        services.AddScoped<UserSyncStatusService>();
        services.AddScoped<ISummitsCalculator>(_ => new SummitsCalculatorWithBoundingBoxFilter());
        services.AddSingleton<RaceDiscoveryService>();
        services.AddSingleton<DiscoverDuvRaces>();
        services.AddSingleton<DiscoverTraceDeTrailRaces>();
        services.AddSingleton<DiscoverItraRaces>();
        services.AddSingleton<DiscoverTrailrunningSwedenRaces>();
        services.AddSingleton<DiscoverSkyrunningRaces>();
        services.AddSingleton<DiscoverLopplistanRaces>();
        services.AddSingleton<DiscoverMittloppRaces>();

        // P2: per-queue transport swap (HANGFIRE_QUEUES). Hangfire storage is
        // registered once by each host entry point, not here.
        QueueTransport.AddHangfireQueueProducer(services);

    }
}
