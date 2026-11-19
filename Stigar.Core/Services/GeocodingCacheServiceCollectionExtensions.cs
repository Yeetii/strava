using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Stigar.Core.Constants;
using StackExchange.Redis;

namespace Stigar.Core.Services;

public static class GeocodingCacheServiceCollectionExtensions
{
    public static IServiceCollection AddSharedGeocodingCache(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton(serviceProvider => LoadOptions(configuration));

        if (!services.Any(descriptor => descriptor.ServiceType == typeof(IConnectionMultiplexer)))
        {
            var connectionString = configuration[AppConfig.RedisConnectionString]
                ?? configuration.GetConnectionString(AppConfig.RedisConnectionString)
                ?? "redis:6379,abortConnect=false";
            services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(connectionString));
        }

        if (!services.Any(descriptor => descriptor.ServiceType == typeof(IDatabase)))
        {
            services.AddSingleton<IDatabase>(serviceProvider =>
                serviceProvider.GetRequiredService<IConnectionMultiplexer>().GetDatabase());
        }

        services.AddSingleton<IGeocodingCache, RedisGeocodingCache>();
        return services;
    }

    private static GeocodingCacheOptions LoadOptions(IConfiguration configuration)
        => configuration.GetSection("GeocodingCache").Get<GeocodingCacheOptions>() ?? new GeocodingCacheOptions();
}
