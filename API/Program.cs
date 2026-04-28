using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker.Extensions.OpenApi.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using Shared.Constants;
using System.Net.Http.Headers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

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
            var cosmosClient = serviceProvider.GetRequiredService<CosmosClient>();
            var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.RacesContainer);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            return new RaceCollectionClient(container, loggerFactory);
        });

        services.AddSingleton(serviceProvider =>
        {
            var cosmosClient = serviceProvider.GetRequiredService<CosmosClient>();
            var container = cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.RaceOrganizersContainer);
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            return new RaceOrganizerClient(container, loggerFactory);
        });

        services.AddScoped(serviceProvider =>
        {
            var httpClient = serviceProvider.GetRequiredService<HttpClient>();
            return new AuthenticationApi(httpClient, configuration);
        });
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
        services.AddScoped(serviceProvider =>
        {
            var usersCollection = serviceProvider.GetRequiredService<CollectionClient<Shared.Models.User>>();
            var sessionsCollection = serviceProvider.GetRequiredService<CollectionClient<Session>>();
            return new UserAuthenticationService(usersCollection, sessionsCollection);
        });
        services.AddScoped<UserSyncService>();
    })
    .ConfigureOpenApi()
    .Build();

host.Run();
