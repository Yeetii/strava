using Hangfire;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;
using Stigar.Workers;

namespace Stigar.Workers.Tests;

/// <summary>
/// Pins the deployment invariants behind the failing Hangfire recurring jobs:
/// the registered target must be loadable by hosts that only reference
/// Stigar.Core, and the activity polling contract must stay registered in the
/// shared Backend services because the schedule role consumes backend-polling.
/// </summary>
public class RecurringJobHostCompatibilityTests
{
    [Fact]
    public async Task RecurringJobTargetsLiveInTheSharedCoreAssembly()
    {
        var manager = new Mock<IRecurringJobManager>();
        Hangfire.Common.Job? captured = null;

        manager
            .Setup(m => m.AddOrUpdate(
                Moq.It.IsAny<string>(),
                Moq.It.IsAny<Hangfire.Common.Job>(),
                Moq.It.IsAny<string>(),
                Moq.It.IsAny<RecurringJobOptions>()))
            .Callback<string, Hangfire.Common.Job, string, RecurringJobOptions>(
                (_, job, _, _) => captured = job);

        var service = new RecurringJobRegistrationHostedService(
            [new ProbeRecurringJob()],
            manager.Object,
            NullLogger<RecurringJobRegistrationHostedService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.NotNull(captured);
        Assert.Equal(typeof(IRecurringJobDispatcher), captured!.Type);
        Assert.Equal(typeof(IRecurringJob).Assembly, captured.Type.Assembly);
    }

    [Fact]
    public async Task DispatcherRunsTheImplementationMatchingTheJobId()
    {
        var first = new ProbeRecurringJob();
        var second = new SecondProbeRecurringJob();

        var dispatcher = new RecurringJobDispatcher(
            [first, second], NullLogger<RecurringJobDispatcher>.Instance);

        await dispatcher.RunAsync(second.JobId);

        Assert.Equal(0, first.Executions);
        Assert.Equal(1, second.Executions);
        await Assert.ThrowsAsync<InvalidOperationException>(() => dispatcher.RunAsync("unknown"));
    }

    [Fact]
    public void BackendServicesRegistersActivityPollingContractForEveryRole()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "strava-recurring-di-tests", Guid.NewGuid().ToString("N"));
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

        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IQueueActivityJobs));
        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(QueueActivityJobs));
        Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IRecurringJobDispatcher));
    }

    private sealed class ProbeRecurringJob : IRecurringJob
    {
        public string JobId => "probe";
        public string Cron => "0 0 * * *";
        public int Executions { get; private set; }

        public Task ExecuteAsync(CancellationToken cancellationToken)
        {
            Executions++;
            return Task.CompletedTask;
        }
    }

    private sealed class SecondProbeRecurringJob : IRecurringJob
    {
        public string JobId => "probe-2";
        public string Cron => "0 1 * * *";
        public int Executions { get; private set; }

        public Task ExecuteAsync(CancellationToken cancellationToken)
        {
            Executions++;
            return Task.CompletedTask;
        }
    }
}
