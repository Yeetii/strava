using System.Net.Http.Headers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Stigar.Core.Constants;
using Stigar.Core.Geo.SummitsCalculator;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Admin;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;
using Stigar.Core.Services.Shards;
using Stigar.Core.Services.StravaClient;

namespace Stigar.Core.Services;

/// <summary>
/// The DI graph shared by the API and Workers hosts. Keeping these registrations
/// in one place prevents the hosts from drifting, most importantly on service
/// lifetimes. Host-only concerns (HTTP JSON options, Hangfire/Redis storage,
/// CORS, geocoding and role-specific workers) stay with their host.
/// </summary>
public static class StigarSharedServiceCollectionExtensions
{
    public static IServiceCollection AddStigarSharedServices(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        var socketsHttpHandler = new SocketsHttpHandler();
        services.AddSingleton(socketsHttpHandler);
        services.AddHttpClient();

        services.AddHttpClient("stravaClient", client =>
            client.BaseAddress = new Uri("https://www.strava.com/api/v3/"));
        services.AddHttpClient<OverpassClient>(client =>
        {
            client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
            client.Timeout = TimeSpan.FromMinutes(10);
        });

        string postgresConnectionString = configuration.GetValue<string>(DatabaseConfig.PostgresConnectionStringKey)
            ?? "Host=localhost;Port=5432;Database=strava;Username=strava;Password=strava;GSS Encryption Mode=Disable";
        services.AddSingleton(NpgsqlDataSource.Create(postgresConnectionString));
        services.AddPostgresCoreRepositories();
        services.AddPostgresSpatialRepositories();
        services.AddPostgresOsmIngestion();
        services.AddPostgresAdminQueries();
        services.AddBackendAdminQueries();

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
        services.AddScoped(serviceProvider => new AuthenticationApi(
            serviceProvider.GetRequiredService<IHttpClientFactory>().CreateClient(),
            configuration));
        services.AddScoped<StravaTokenService>();
        services.AddScoped(serviceProvider => new UserAuthenticationService(
            serviceProvider.GetRequiredService<IUserRepository>(),
            serviceProvider.GetRequiredService<ISessionRepository>()));
        services.AddScoped<UserSyncStatusService>();

        // SummitsCalculatorWithBoundingBoxFilter is stateless. One singleton
        // registration for both hosts removes the old Singleton/Scoped drift.
        services.AddSingleton<ISummitsCalculator, SummitsCalculatorWithBoundingBoxFilter>();

        services.AddSingleton<RaceDiscoveryService>();
        services.AddSingleton(serviceProvider => new RaceScrapePipeline(
            serviceProvider.GetRequiredService<IHttpClientFactory>(),
            serviceProvider.GetRequiredService<IOrganizerStore>(),
            serviceProvider.GetRequiredService<ILoggerFactory>().CreateLogger("RaceScrapePipeline")));

        return services;
    }
}
