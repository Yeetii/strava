using Hangfire;
using Microsoft.Extensions.DependencyInjection;
using Hangfire.Console;
using Hangfire.Redis.StackExchange;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Stigar.Core.Jobs;

/// <summary>Hangfire storage/console wiring shared by every host.</summary>
public static class HangfireInfrastructure
{
    /// <summary>
    /// Config key for the number of concurrent Hangfire workers per host.
    /// Set it in the environment as <c>Hangfire__WorkerCount</c>.
    /// </summary>
    public const string WorkerCountKey = "Hangfire:WorkerCount";

    public const string DefaultRedisKeyPrefix = "{hangfire}:";

    public static string GetRedisKeyPrefix(IConfiguration configuration) =>
        configuration["Hangfire:RedisPrefix"] ?? DefaultRedisKeyPrefix;

    /// <summary>Fallback concurrency when no valid value is configured.</summary>
    public const int DefaultWorkerCount = 8;

    // UseConsole() mutates global Hangfire state and throws when called twice.
    private static int _consoleInitialized;

    /// <summary>
    /// Reads the configured worker count, falling back to <see cref="DefaultWorkerCount"/>
    /// when the value is missing, unparseable or not positive.
    /// </summary>
    public static int GetWorkerCount(IConfiguration configuration)
    {
        var value = configuration[WorkerCountKey];
        return int.TryParse(value, out var configured) && configured > 0
            ? configured
            : DefaultWorkerCount;
    }

    public static void AddHangfireStorage(IServiceCollection services, IConfiguration configuration)
    {
        var redis = configuration.GetValue<string>("RedisConnectionString") ?? "redis:6379,abortConnect=false";
        services.AddHangfire(config =>
        {
            config.SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
                .UseSimpleAssemblyNameTypeSerializer()
                .UseRecommendedSerializerSettings()
                .UseRedisStorage(redis, new RedisStorageOptions
                {
                    Prefix = GetRedisKeyPrefix(configuration)
                });

            if (Interlocked.Exchange(ref _consoleInitialized, 1) == 0)
                config.UseConsole();
        });

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILoggerProvider, HangfireConsoleLoggerProvider>());
    }
}
