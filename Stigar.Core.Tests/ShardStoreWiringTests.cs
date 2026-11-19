using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Stigar.Core.Constants;
using Stigar.Core.Services;
using Stigar.Core.Services.Shards;

namespace Stigar.Core.Tests;

/// <summary>
/// RED contract suite for T4: host wiring selects the shard stores by configuration,
/// mirroring AddOrganizerStore. References the not-yet-existing AddShardStore extension
/// and config constants, so the build must fail until GREEN.
/// </summary>
public sealed class ShardStoreWiringTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "strava-shard-wiring", Guid.NewGuid().ToString("N"));

    public ShardStoreWiringTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        if (Directory.Exists(_root))
            Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public void AddShardStore_FileSystemProvider_ResolvesFilesystemStores()
    {
        using var provider = BuildProvider();

        Assert.IsType<FileSystemShardRepository>(provider.GetRequiredService<IShardRepository>());
        Assert.IsType<FileSystemShardIndexStore>(provider.GetRequiredService<IShardIndexStore>());
    }

    [Fact]
    public void AddShardStore_FileSystemProvider_ResolvesFullShardStack()
    {
        using var provider = BuildProvider(registerStack: true);

        Assert.NotNull(provider.GetRequiredService<ShardFeatureClient>());
        Assert.NotNull(provider.GetRequiredService<HighwayZoomIndexService>());
        Assert.NotNull(provider.GetRequiredService<BlobTileService>());
    }

    [Fact]
    public void AddShardStore_FileSystemProvider_WithoutRootDir_Throws()
    {
        var services = CreateBaseServices();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [AppConfig.ShardStoreProvider] = ShardStoreProviders.FileSystem
            })
            .Build();

        Assert.Throws<InvalidOperationException>(() => services.AddShardStore(configuration));
    }

    [Fact]
    public void AddShardStore_BlobProvider_ThrowsNotSupported()
    {
        var services = CreateBaseServices();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [AppConfig.ShardStoreProvider] = ShardStoreProviders.Blob
            })
            .Build();

        Assert.Throws<NotSupportedException>(() => services.AddShardStore(configuration));
    }

    private ServiceProvider BuildProvider(bool registerStack = false)
    {
        var services = CreateBaseServices();
        var values = new Dictionary<string, string?>
        {
            [AppConfig.ShardStoreProvider] = ShardStoreProviders.FileSystem,
            [AppConfig.ShardStoreRootDir] = _root,
        };

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        services.AddShardStore(configuration);

        if (registerStack)
        {
            services.AddSingleton<ShardFeatureClient>();
            services.AddSingleton(sp => new HighwayZoomIndexService(
                sp.GetRequiredService<IShardIndexStore>(),
                sp.GetRequiredService<IShardRepository>(),
                sp.GetRequiredService<ILogger<HighwayZoomIndexService>>(),
                canonicalZoom: 12));
            services.AddSingleton<BlobTileService>();
        }

        return services.BuildServiceProvider();
    }

    private static IServiceCollection CreateBaseServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(new HttpClient());
        services.AddSingleton<OverpassClient>();
        return services;
    }
}
