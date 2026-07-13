using Azure.Data.Tables;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Services;

public static class GeocodingCacheServiceCollectionExtensions
{
    public static IServiceCollection AddSharedGeocodingCache(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton(serviceProvider =>
        {
            var cacheOptions = LoadOptions(configuration);
            var connectionString = ResolveStorageConnectionString(configuration);
            return new TableServiceClient(connectionString).GetTableClient(cacheOptions.TableName);
        });
        services.AddSingleton(serviceProvider => LoadOptions(configuration));
        services.AddSingleton<IGeocodingCache, TableGeocodingCache>();
        services.AddHostedService<GeocodingCacheTableInitializer>();
        return services;
    }

    private static GeocodingCacheOptions LoadOptions(IConfiguration configuration)
        => configuration.GetSection("GeocodingCache").Get<GeocodingCacheOptions>() ?? new GeocodingCacheOptions();

    private static string ResolveStorageConnectionString(IConfiguration configuration)
        => configuration.GetConnectionString("AzureWebJobsStorage")
            ?? configuration["AzureWebJobsStorage"]
            ?? configuration.GetConnectionString("BlobStorageConnection")
            ?? configuration["BlobStorageConnection"]
            ?? throw new InvalidOperationException("No Azure Storage connection string found for geocoding cache.");

    private sealed class GeocodingCacheTableInitializer(TableClient tableClient, ILogger<GeocodingCacheTableInitializer> logger) : IHostedService
    {
        private readonly TableClient _tableClient = tableClient;
        private readonly ILogger<GeocodingCacheTableInitializer> _logger = logger;

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await _tableClient.CreateIfNotExistsAsync(cancellationToken);
            _logger.LogInformation("Ensured geocoding cache table '{TableName}' exists", _tableClient.Name);
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
