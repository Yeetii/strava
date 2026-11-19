using Hangfire;
using Microsoft.Extensions.DependencyInjection;
using Hangfire.Console;
using Hangfire.Redis.StackExchange;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Shared.Jobs;

/// <summary>Hangfire storage/console wiring shared by every host.</summary>
public static class HangfireInfrastructure
{
    // UseConsole() mutates global Hangfire state and throws when called twice.
    private static int _consoleInitialized;

    public static void AddHangfireStorage(IServiceCollection services, IConfiguration configuration)
    {
        var redis = configuration.GetValue<string>("RedisConnectionString") ?? "redis:6379,abortConnect=false";
        services.AddHangfire(config =>
        {
            config.SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
                .UseSimpleAssemblyNameTypeSerializer()
                .UseRecommendedSerializerSettings()
                .UseRedisStorage(redis);

            if (Interlocked.Exchange(ref _consoleInitialized, 1) == 0)
                config.UseConsole();
        });

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILoggerProvider, HangfireConsoleLoggerProvider>());
    }
}
