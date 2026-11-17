using API;
using API.Utils;
using Hangfire;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Azure.Monitor.Query;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.GarminClient;
using Shared.Services.Shards;
using Shared.Services.StravaClient;
using Shared.Hosting;

// ASP.NET Core (Kestrel) host replacing the Functions worker for the API
// container. All 59 endpoints are mapped from their existing function bodies
// by MapFunctions(); the DI graph below transfers verbatim from the old
// ConfigureServices block.

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<JsonSerializerOptions>(options =>
{
    options.PropertyNameCaseInsensitive = true;
    options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.Converters.Add(new GeometrySystemTextJsonConverter());
    options.Converters.Add(new JsonStringEnumConverter());
    options.Converters.Add(new FeatureIdJsonConverter());
});

var socketsHttpHandler = new SocketsHttpHandler();
builder.Services.AddSingleton(socketsHttpHandler);
builder.Services.AddHttpClient();

var configuration = builder.Configuration;
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
string apiCosmosDbConnectionString = configuration.GetValue<string>("CosmosDBConnection")
    ?? throw new Exception("No cosmos connection string found");
builder.Services.AddSingleton(new CosmosClient(apiCosmosDbConnectionString, apiCosmosClientOptions));

new CollectionClientBuilder(builder.Services)
    .AddOsmFeatureCaches(DatabaseConfig.CosmosDb, DatabaseConfig.OsmFeaturesContainer)
    .AddCollection<SummitedPeak>(DatabaseConfig.CosmosDb, DatabaseConfig.SummitedPeaksContainer)
    .AddCollection<Shared.Models.User>(DatabaseConfig.CosmosDb, DatabaseConfig.UsersContainer)
    .AddCollection<UserSyncItem>(DatabaseConfig.CosmosDb, DatabaseConfig.UserSyncItemsContainer)
    .AddCollection<Session>(DatabaseConfig.CosmosDb, DatabaseConfig.SessionsContainer)
    .AddCollection<Activity>(DatabaseConfig.CosmosDb, DatabaseConfig.ActivitiesContainer)
    .AddCollection<VisitedPath>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedPathsContainer)
    .AddCollection<VisitedArea>(DatabaseConfig.CosmosDb, DatabaseConfig.VisitedAreasContainer);

builder.Services.AddSingleton(serviceProvider =>
{
    var blobConnection = configuration.GetValue<string>("BlobStorageConnection")
        ?? throw new InvalidOperationException("BlobStorageConnection is not configured.");
    var containerClient = new BlobContainerClient(blobConnection, BlobContainerNames.RaceOrganizers);
    containerClient.CreateIfNotExists();
    var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
    return new BlobOrganizerStore(containerClient, loggerFactory);
});
builder.Services.AddSingleton(serviceProvider =>
{
    var blobConnection = configuration.GetValue<string>("BlobStorageConnection")
        ?? throw new InvalidOperationException("BlobStorageConnection is not configured.");
    var containerClient = new BlobContainerClient(blobConnection, BlobContainerNames.HighwaysShards);
    containerClient.CreateIfNotExists();
    return new HighwaysShardContainer(containerClient);
});
builder.Services.AddSingleton<IShardRepository>(serviceProvider =>
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
            await zoomIndexService.SyncIndexesForShardAsync(x, y, shard, cancellationToken);
        });
});
builder.Services.AddSingleton(serviceProvider =>
{
    var shardRepository = serviceProvider.GetRequiredService<IShardRepository>();
    var shardLogger = serviceProvider.GetRequiredService<ILogger<ShardFeatureClient>>();
    var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
    return new ShardFeatureClient(shardRepository, shardLogger, shardZoom);
});
builder.Services.AddSingleton(serviceProvider =>
{
    var containerClient = serviceProvider.GetRequiredService<HighwaysShardContainer>().Client;
    var shardRepository = serviceProvider.GetRequiredService<IShardRepository>();
    var indexLogger = serviceProvider.GetRequiredService<ILogger<HighwayZoomIndexService>>();
    var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
    return new HighwayZoomIndexService(containerClient, shardRepository, indexLogger, shardZoom);
});
builder.Services.AddSingleton(serviceProvider =>
{
    var featureClient = serviceProvider.GetRequiredService<ShardFeatureClient>();
    var zoomIndexService = serviceProvider.GetRequiredService<HighwayZoomIndexService>();
    var tileLogger = serviceProvider.GetRequiredService<ILogger<BlobTileService>>();
    var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
    return new BlobTileService(featureClient, zoomIndexService, tileLogger, shardZoom);
});

builder.Services.AddScoped(serviceProvider =>
{
    var httpClient = serviceProvider.GetRequiredService<HttpClient>();
    return new AuthenticationApi(httpClient, configuration);
});
builder.Services.AddHttpClient("stravaClient", client =>
{
    client.BaseAddress = new Uri("https://www.strava.com/api/v3/");
});
builder.Services.AddHttpClient("garminProxyClient", client =>
{
    var garminProxyBaseUrl = configuration.GetValue<string>("GarminProxyBaseUrl")
        ?? "http://localhost:7073/api/";
    client.BaseAddress = new Uri(garminProxyBaseUrl);
    var functionsKey = configuration.GetValue<string>("LIFEDASH_FUNCTIONS_KEY");
    if (!string.IsNullOrWhiteSpace(functionsKey))
        client.DefaultRequestHeaders.TryAddWithoutValidation("x-functions-key", functionsKey);
});
builder.Services.AddSingleton(serviceProvider =>
{
    var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
    var stravaClient = httpClientFactory.CreateClient("stravaClient");
    return new RoutesApi(stravaClient);
});
builder.Services.AddSingleton(serviceProvider =>
{
    var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
    var garminProxyClient = httpClientFactory.CreateClient("garminProxyClient");
    return new GarminCoursesApi(garminProxyClient);
});
builder.Services.AddScoped<StravaTokenService>();
builder.Services.AddHttpClient<OverpassClient>(client =>
{
    client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
    client.Timeout = TimeSpan.FromMinutes(10);
});
builder.Services.AddSingleton(_ =>
    new ServiceBusClient(configuration.GetValue<string>("ServiceBusConnection")));
builder.Services.AddSingleton(_ =>
    new ServiceBusAdministrationClient(configuration.GetValue<string>("ServiceBusConnection")));

// P2: per-queue transport swap. Storage is registered once per host; the
// producer shim is added only when HANGFIRE_QUEUES names a queue (or "*").
QueueTransport.AddHangfireStorage(builder.Services, configuration);
QueueTransport.AddHangfireQueueProducer(builder.Services);
builder.Services.AddSingleton(new MetricsQueryClient(new DefaultAzureCredential()));
builder.Services.AddScoped(serviceProvider =>
{
    var usersCollection = serviceProvider.GetRequiredService<CollectionClient<Shared.Models.User>>();
    var sessionsCollection = serviceProvider.GetRequiredService<CollectionClient<Session>>();
    return new UserAuthenticationService(usersCollection, sessionsCollection);
});
builder.Services.AddScoped<UserSyncService>();
builder.Services.AddScoped<UserSyncStatusService>();

// Azure SignalR stays; negotiate is produced by the same code path so the
// response shape is unchanged. See SignalRNegotiate.
builder.Services.AddSingleton<Func<string, SignalRConnectionInfo>>(_ =>
    (Func<string, SignalRConnectionInfo>)(sessionId => SignalRNegotiate.Create(
        configuration.GetValue<string>("AzureSignalRConnectionString")
        ?? throw new InvalidOperationException("AzureSignalRConnectionString is not configured."),
        sessionId)));

var allowedOrigins = configuration.GetValue<string>("AllowedOrigins")?
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    ?? [];
builder.Services.AddCors(options => options.AddDefaultPolicy(policy =>
{
    if (allowedOrigins.Length > 0)
        policy.WithOrigins(allowedOrigins).AllowAnyHeader().AllowAnyMethod().AllowCredentials();
    else
        policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
}));
builder.Services.AddOpenApi();

var app = builder.Build();

FunctionsJson.Options = app.Services.GetRequiredService<IOptions<JsonSerializerOptions>>().Value;

app.UseCors();
app.MapOpenApi();

// Hangfire dashboard is tailnet-only: Funnel traffic carries a strip-proof
// header and is rejected outright; everything else needs the admin key via
// HTTP Basic auth. Fail closed when AdminApiKey is not configured.
app.Use(async (context, next) =>
{
    if (context.Request.Path.StartsWithSegments(
            HangfireDashboardGuard.DashboardPath, StringComparison.OrdinalIgnoreCase))
    {
        if (HangfireDashboardGuard.IsFunnelRequest(context) ||
            !HangfireDashboardGuard.IsAuthorized(context, configuration["AdminApiKey"]))
        {
            context.Response.Headers.WWWAuthenticate = "Basic realm=\"Hangfire\"";
            context.Response.StatusCode = StatusCodes.Status401Unauthorized;
            return;
        }
    }

    await next(context);
});
// Authorization = [] disables Hangfire's local-only default; the middleware
// above is the sole gate for this route and must stay in place.
app.UseHangfireDashboard(HangfireDashboardGuard.DashboardPath, new DashboardOptions
{
    Authorization = [],
    DisplayStorageConnectionString = false,
    DashboardTitle = "Peakshunters Jobs",
});

app.MapFunctions(typeof(Program).Assembly);

app.MapGet("/health", () => Results.Ok("ok"));

app.Run();
