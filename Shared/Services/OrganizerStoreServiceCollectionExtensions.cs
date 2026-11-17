using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Constants;

namespace Shared.Services;

public static class OrganizerStoreServiceCollectionExtensions
{
    public static IServiceCollection AddOrganizerStore(this IServiceCollection services, IConfiguration configuration)
    {
        var provider = configuration[AppConfig.OrganizerStoreProvider];
        if (string.Equals(provider, OrganizerStoreProviders.FileSystem, StringComparison.OrdinalIgnoreCase))
        {
            var root = configuration[AppConfig.RaceDataDir];
            if (string.IsNullOrWhiteSpace(root))
            {
                throw new InvalidOperationException(
                    $"'{AppConfig.RaceDataDir}' must be configured when the {OrganizerStoreProviders.FileSystem} organizer store is selected.");
            }

            services.AddSingleton<IOrganizerStore>(sp =>
                new FileSystemOrganizerStore(root, sp.GetRequiredService<ILoggerFactory>()));
            return services;
        }

        // Blob is the default so existing deployments keep their current behaviour.
        var connectionString = ResolveBlobConnectionString(configuration);
        services.AddSingleton<IOrganizerStore>(sp => new BlobOrganizerStore(
            new BlobContainerClient(connectionString, BlobContainerNames.RaceOrganizers),
            sp.GetRequiredService<ILoggerFactory>()));
        return services;
    }

    private static string ResolveBlobConnectionString(IConfiguration configuration)
        => configuration.GetConnectionString("BlobStorageConnection")
            ?? configuration["BlobStorageConnection"]
            ?? throw new InvalidOperationException(
                "No Azure Storage connection string found for the blob organizer store.");
}
