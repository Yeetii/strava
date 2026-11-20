using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Stigar.Core.Services;
using Stigar.Core.Services.StravaClient;
using Stigar.Core.Jobs;

namespace Stigar.Workers;

/// <summary>
/// The Backend DI graph used by the worker-schedule and worker-heavy hosts.
/// Graph shared with the API host lives in
/// <see cref="StigarSharedServiceCollectionExtensions.AddStigarSharedServices"/>.
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

        services.AddStigarSharedServices(configuration);

        // Worker-role-only services.
        services.AddSingleton(serviceProvider =>
        {
            var factory = serviceProvider.GetRequiredService<IHttpClientFactory>();
            return new ActivitiesApi(factory.CreateClient("stravaClient"));
        });
        services.AddHttpClient<ILocationGeocodingService, NominatimLocationGeocodingService>(client =>
        {
            client.BaseAddress = new Uri("https://nominatim.openstreetmap.org/");
            client.DefaultRequestHeaders.UserAgent.Add(new System.Net.Http.Headers.ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
            client.Timeout = TimeSpan.FromSeconds(10);
        });
        services.AddSharedGeocodingCache(configuration);

        services.AddSingleton<DiscoverDuvRaces>();
        services.AddSingleton<DiscoverTraceDeTrailRaces>();
        services.AddSingleton<DiscoverItraRaces>();
        services.AddSingleton<DiscoverTrailrunningSwedenRaces>();
        services.AddSingleton<DiscoverSkyrunningRaces>();
        services.AddSingleton<DiscoverLopplistanRaces>();
        services.AddSingleton<DiscoverMittloppRaces>();

        // Hangfire targets this shared interface; the concrete IRecurringJob
        // implementations stay here in the executing host.
        services.AddSingleton<IRecurringJobDispatcher, RecurringJobDispatcher>();

        // The schedule worker is the sole consumer of the backend-polling queue,
        // so the activity polling contract must be resolvable on that role.
        // Hangfire resolves the target interface from DI before invoking the job.
        services.AddSingleton<QueueActivityJobs>();
        services.AddSingleton<IQueueActivityJobs>(sp =>
            sp.GetRequiredService<QueueActivityJobs>());
        services.AddSingleton<IAdminStatusSnapshotJob, AdminStatusSnapshotJob>();

        // Hangfire storage is registered by each host entry point, not here.
    }
}
