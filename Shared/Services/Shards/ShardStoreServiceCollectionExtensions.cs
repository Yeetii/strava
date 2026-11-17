using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;

namespace Shared.Services.Shards;

/// <summary>
/// Selects the shard and zoom-index stores by configuration, mirroring
/// <see cref="OrganizerStoreServiceCollectionExtensions.AddOrganizerStore"/>.
/// Blob remains the default; the self-hosted stack opts into FileSystem.
/// </summary>
public static class ShardStoreServiceCollectionExtensions
{
    public static IServiceCollection AddShardStore(this IServiceCollection services, IConfiguration configuration)
    {
        var shardZoom = configuration.GetValue<int?>(AppConfig.BlobShardZoom) ?? 12;
        var shardBufferMeters = configuration.GetValue<int?>(AppConfig.BlobShardBufferMeters) ?? 200;

        // Resolve the index service only when a shard actually changes; registering it
        // directly here would create a construction cycle with IShardRepository.
        static Func<IServiceProvider, Func<int, int, Shard?, CancellationToken, Task>> ChangedFactory()
            => serviceProvider => async (x, y, shard, cancellationToken) =>
                await serviceProvider.GetRequiredService<HighwayZoomIndexService>()
                    .SyncIndexesForShardAsync(x, y, shard, cancellationToken);

        var onShardChanged = ChangedFactory();

        var provider = configuration[AppConfig.ShardStoreProvider];
        if (string.Equals(provider, ShardStoreProviders.FileSystem, StringComparison.OrdinalIgnoreCase))
        {
            var root = configuration[AppConfig.ShardStoreRootDir];
            if (string.IsNullOrWhiteSpace(root))
            {
                throw new InvalidOperationException(
                    $"'{AppConfig.ShardStoreRootDir}' must be configured when the {ShardStoreProviders.FileSystem} shard store is selected.");
            }

            services.AddSingleton<IShardIndexStore>(new FileSystemShardIndexStore(root));
            services.AddSingleton<IShardRepository>(sp => new FileSystemShardRepository(
                root,
                sp.GetRequiredService<ILogger<FileSystemShardRepository>>(),
                sp.GetRequiredService<OverpassClient>().GetHighways,
                shardZoom,
                shardBufferMeters,
                onShardChanged(sp)));
            return services;
        }

        var connection = configuration.GetConnectionString("BlobStorageConnection")
            ?? configuration["BlobStorageConnection"]
            ?? throw new InvalidOperationException("BlobStorageConnection is not configured.");

        var container = new BlobContainerClient(connection, BlobContainerNames.HighwaysShards);
        container.CreateIfNotExists();
        services.AddSingleton<IShardIndexStore>(new BlobShardIndexStore(container));
        services.AddSingleton<IShardRepository>(sp => new BlobShardRepository(
            container,
            sp.GetRequiredService<ILogger<BlobShardRepository>>(),
            sp.GetRequiredService<OverpassClient>().GetHighways,
            shardZoom,
            shardBufferMeters,
            onShardChanged(sp)));
        return services;
    }
}
