using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Stigar.Core.Constants;

namespace Stigar.Core.Services;

public static class OrganizerStoreServiceCollectionExtensions
{
    public static IServiceCollection AddOrganizerStore(this IServiceCollection services, IConfiguration configuration)
    {
        var provider = configuration[AppConfig.OrganizerStoreProvider];
        if (string.Equals(provider, OrganizerStoreProviders.Blob, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException("Azure Blob storage for race organizers has been removed. Use FileSystem.");
        }

        var root = configuration[AppConfig.RaceDataDir];
        if (string.IsNullOrWhiteSpace(root))
        {
            throw new InvalidOperationException($"'{AppConfig.RaceDataDir}' must be configured.");
        }

        services.AddSingleton<IOrganizerStore>(sp =>
            new FileSystemOrganizerStore(root, sp.GetRequiredService<ILoggerFactory>()));
        return services;
    }
}
