using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Monitor.Query;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker.Extensions.OpenApi.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.GarminClient;
using Shared.Services.StravaClient;
using Shared.Constants;
using System.Net.Http.Headers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Shared.Services.Shards;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults(worker =>
    {
        worker.UseMiddleware<API.Utils.CorsMiddleware>();
    })
    .ConfigureServices((hostingContext, services) =>
    {
        var configuration = hostingContext.Configuration;
        services.Configure<JsonSerializerOptions>(options =>
        {
            options.PropertyNameCaseInsensitive = true;
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
            options.Converters.Add(new GeometrySystemTextJsonConverter());
            options.Converters.Add(new JsonStringEnumConverter());
            options.Converters.Add(new FeatureIdJsonConverter());
        });

        var socketsHttpHandler = new SocketsHttpHandler();
        services.AddSingleton(socketsHttpHandler);
        services.AddHttpClient();

        var configuredConnectionMode = configuration.GetValue<string>("CosmosConnectionMode");
        var connectionMode = Enum.TryParse<ConnectionMode>(configuredConnectionMode, ignoreCase: true, out var parsedConnectionMode)
            ? parsedConnectionMode
            : ConnectionMode.Direct;
        CosmosClientOptions apiCosmosClientOptions = new()
        {
            HttpClientFactory = () => new HttpClient(socketsHttpHandler, disposeHandler: false),
            SerializerOptions = new CosmosSerializationOptions { PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase },
            ConnectionMode = connectionMode,
            AllowBulkExecution = true,
            MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes(1),
            MaxRetryAttemptsOnRateLimitedRequests = 5
        };
        string apiCosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection") ?? throw new Exception("No cosmos connection string found");
        // Register as instance so the DI container does not own or dispose it during host shutdown.
        // Using a factory (AddSingleton(factory)) would cause the container to call Dispose() when
        // the host recycles, failing any in-flight Cosmos operations with ObjectDisposedException.
        services.AddSingleton(new CosmosClient(apiCosmosDbConnectionString, apiCosmosClientOptions));


        new CollectionClientBuilder(services)
            .AddOsmFeatureCaches(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer)
            .AddCollection<SummitedPeak>(DatabaseConfig.CosmosDb, DatabaseConfig.SummitedPeaksContainer)
            .AddCollection<Shared.Models.User>(DatabaseConfig.CosmosDb, DatabaseConfig.UsersContainer)
            .AddCollection<UserSyncItem>(DatabaseConfig.CosmosDb, DatabaseConfig.UserSyncItemsContainer)
            .AddCollection<Session>(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer)
            .AddCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer)
            .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
            .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer);

        services.AddSingleton(serviceProvider =>
        {
            var blobConnection = configuration.GetValue<string>("BlobStorageConnection")
                ?? throw new InvalidOperationException("BlobStorageConnection is not configured.");
            var containerClient = new BlobContainerClient(blobConnection, BlobContainerNames.RaceOrganizers);
            containerClient.CreateIfNotExists();
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            return new BlobOrganizerStore(containerClient, loggerFactory);
        });
        services.AddSingleton(serviceProvider =>
        {
            var blobConnection = configuration.GetValue<string>("BlobStorageConnection")
                ?? throw new InvalidOperationException("BlobStorageConnection is not configured.");
            var containerClient = new BlobContainerClient(blobConnection, BlobContainerNames.HighwaysShards);
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
                containerClient,
                repositoryLogger,
                overpass.GetHighways,
                shardZoom,
                shardBufferMeters,
                async (x, y, shard, cancellationToken) =>
                {
                    var zoomIndexService = serviceProvider.GetRequiredService<HighwayZoomIndexService>();
                    await zoomIndexService.UpdateIndexesForShardAsync(x, y, shard, cancellationToken);
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
        services.AddSingleton(serviceProvider =>
        {
            var featureClient = serviceProvider.GetRequiredService<ShardFeatureClient>();
            var zoomIndexService = serviceProvider.GetRequiredService<HighwayZoomIndexService>();
            var tileLogger = serviceProvider.GetRequiredService<ILogger<BlobTileService>>();
            var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
            return new BlobTileService(featureClient, zoomIndexService, tileLogger, shardZoom);
        });

        services.AddScoped(serviceProvider =>
        {
            var httpClient = serviceProvider.GetRequiredService<HttpClient>();
            return new AuthenticationApi(httpClient, configuration);
        });
        services.AddHttpClient("stravaClient", client =>
        {
            client.BaseAddress = new Uri("https://www.strava.com/api/v3/");
        });
        services.AddHttpClient("garminProxyClient", client =>
        {
            var garminProxyBaseUrl = configuration.GetValue<string>("GarminProxyBaseUrl")
                ?? "http://localhost:7073/api/";
            client.BaseAddress = new Uri(garminProxyBaseUrl);
            var functionsKey = configuration.GetValue<string>("LIFEDASH_FUNCTIONS_KEY");
            if (!string.IsNullOrWhiteSpace(functionsKey))
                client.DefaultRequestHeaders.TryAddWithoutValidation("x-functions-key", functionsKey);
        });
        services.AddSingleton(serviceProvider =>
        {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var stravaClient = httpClientFactory.CreateClient("stravaClient");
            return new RoutesApi(stravaClient);
        });
        services.AddSingleton(serviceProvider =>
        {
            var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            var garminProxyClient = httpClientFactory.CreateClient("garminProxyClient");
            return new GarminCoursesApi(garminProxyClient);
        });
        services.AddScoped<StravaTokenService>();
        services.AddHttpClient<OverpassClient>(
            client =>
            {
                client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
                client.Timeout = TimeSpan.FromMinutes(10); // large country geometries (Russia, Canada) can take several minutes
            });
        services.AddSingleton(serviceProvider =>
        {
            var sbConnectionString = configuration.GetValue<string>("ServicebusConnection");
            return new ServiceBusClient(sbConnectionString);
        });
        services.AddSingleton(serviceProvider =>
        {
            var sbConnectionString = configuration.GetValue<string>("ServicebusConnection");
            return new ServiceBusAdministrationClient(sbConnectionString);
        });
        services.AddSingleton(new MetricsQueryClient(new DefaultAzureCredential()));
        services.AddScoped(serviceProvider =>
        {
            var usersCollection = serviceProvider.GetRequiredService<CollectionClient<Shared.Models.User>>();
            var sessionsCollection = serviceProvider.GetRequiredService<CollectionClient<Session>>();
            return new UserAuthenticationService(usersCollection, sessionsCollection);
        });
        services.AddScoped<UserSyncService>();
        services.AddScoped<UserSyncStatusService>();
    })
    .ConfigureOpenApi()
    .Build();

host.Run();
