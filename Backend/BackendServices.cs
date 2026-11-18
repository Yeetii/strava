using System.Configuration;
using System.Net.Http.Headers;
using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Geo.SummitsCalculator;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Spatial;
using Shared.Services.Postgres.Spatial.Ingestion;
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

        // PostgreSQL + PostGIS data source and repositories. The connection
        // string is supplied by the deploy environment; the localhost default
        // keeps local and test hosts startable before Postgres is configured.
        string postgresConnectionString = configuration.GetValue<string>(DatabaseConfig.PostgresConnectionStringKey)
            ?? "Host=localhost;Port=5432;Database=strava;Username=strava;Password=strava";
        services.AddSingleton(NpgsqlDataSource.Create(postgresConnectionString));
        services.AddPostgresCoreRepositories();
        services.AddPostgresSpatialRepositories();
        services.AddPostgresOsmIngestion();

        new CollectionClientBuilder(services)
            .AddCollection<SummitedPeak>(DatabaseConfig.CosmosDb, DatabaseConfig.SummitedPeaksContainer)
            .AddCollection<Shared.Models.User>(DatabaseConfig.CosmosDb, DatabaseConfig.UsersContainer)
            .AddCollection<UserSyncItem>(DatabaseConfig.CosmosDb, DatabaseConfig.UserSyncItemsContainer)
            .AddCollection<Session>(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer)
            .AddCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer)
            .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
            .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer)
            .AddOsmFeatureCaches(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer);

        services.AddOrganizerStore(configuration);
        services.AddShardStore(configuration);
        services.AddSingleton(serviceProvider =>
        {
            var shardRepository = serviceProvider.GetRequiredService<IShardRepository>();
            var shardLogger = serviceProvider.GetRequiredService<ILogger<ShardFeatureClient>>();
            var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
            return new ShardFeatureClient(shardRepository, shardLogger, shardZoom);
        });
        services.AddSingleton(serviceProvider =>
        {
            var indexStore = serviceProvider.GetRequiredService<IShardIndexStore>();
            var shardRepository = serviceProvider.GetRequiredService<IShardRepository>();
            var indexLogger = serviceProvider.GetRequiredService<ILogger<HighwayZoomIndexService>>();
            var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
            return new HighwayZoomIndexService(indexStore, shardRepository, indexLogger, shardZoom);
        });

        services.AddSingleton<AdminBoundaryMetricsEnricher>();

        services.AddScoped(serviceProvider =>
        {
            var factory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            return new AuthenticationApi(factory.CreateClient(), configuration);
        });
        services.AddScoped(serviceProvider =>
            new UserAuthenticationService(
                serviceProvider.GetRequiredService<IUserRepository>(),
                serviceProvider.GetRequiredService<ISessionRepository>()));
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

        // All queues run on Hangfire/Redis. Storage is registered once by each
        // host entry point, not here.
        QueueTransport.AddHangfireQueueProducer(services);

    }
}
