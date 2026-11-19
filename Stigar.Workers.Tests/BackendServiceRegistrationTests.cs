using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Stigar.Core.Constants;
using Stigar.Core.Services;
using Stigar.Core.Services.StravaClient;
using Stigar.Workers;

namespace Stigar.Workers.Tests;

public class BackendServiceRegistrationTests
{
    [Fact]
    public void AddBackendServices_RegistersStravaTokenService()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "strava-worker-di-tests", Guid.NewGuid().ToString("N"));
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [DatabaseConfig.PostgresConnectionStringKey] =
                    "Host=localhost;Database=strava;Username=strava;Password=strava",
                ["StravaClientSecret"] = "test-secret",
                [AppConfig.RaceDataDir] = Path.Combine(tempRoot, "races"),
                [AppConfig.ShardStoreRootDir] = Path.Combine(tempRoot, "shards"),
            })
            .Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddBackendServices(configuration);
        services.AddSingleton<StravaActivityFetcher>();

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<AuthenticationApi>());
        Assert.NotNull(provider.GetService<StravaTokenService>());
        // The exact activation that failed in the worker before StravaTokenService was registered.
        Assert.NotNull(provider.GetService<StravaActivityFetcher>());
    }
}
