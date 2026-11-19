using Hangfire;
using Hangfire.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;

namespace Stigar.Workers.Tests;

public class RecurringJobRegistrationTests
{
    [Fact]
    public async Task StartAsync_TargetsSharedDispatcherWithJobIdArgument()
    {
        var manager = new Mock<IRecurringJobManager>();
        Hangfire.Common.Job? captured = null;
        manager
            .Setup(m => m.AddOrUpdate(
                "probe",
                It.IsAny<Hangfire.Common.Job>(),
                "0 0 * * *",
                It.IsAny<RecurringJobOptions>()))
            .Callback<string, Hangfire.Common.Job, string, RecurringJobOptions>((_, job, _, _) => captured = job);

        var service = new RecurringJobRegistrationHostedService(
            [new ProbeRecurringJob()],
            manager.Object,
            NullLogger<RecurringJobRegistrationHostedService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.NotNull(captured);
        Assert.Equal(typeof(IRecurringJobDispatcher), captured!.Type);
        Assert.Equal(typeof(IRecurringJob).Assembly, captured!.Type.Assembly);
        Assert.Equal("probe", Assert.Single(captured.Args));
        Assert.Equal(AppConfig.RecurringJobsQueueName, captured!.Queue);
    }

    [Fact]
    public async Task StartAsync_RegistersEveryRecurringJobOnTheRecurringQueue()
    {
        var manager = new Mock<IRecurringJobManager>();
        var queues = new List<string?>();
        manager
            .Setup(m => m.AddOrUpdate(
                It.IsAny<string>(),
                It.IsAny<Hangfire.Common.Job>(),
                It.IsAny<string>(),
                It.IsAny<RecurringJobOptions>()))
            .Callback<string, Hangfire.Common.Job, string, RecurringJobOptions>(
                (_, job, _, _) => queues.Add(job.Queue));

        var service = new RecurringJobRegistrationHostedService(
            [new ProbeRecurringJob(), new SecondProbeRecurringJob()],
            manager.Object,
            NullLogger<RecurringJobRegistrationHostedService>.Instance);

        await service.StartAsync(CancellationToken.None);

        Assert.Equal(2, queues.Count);
        Assert.All(queues, queue => Assert.Equal(AppConfig.RecurringJobsQueueName, queue));
    }

    private sealed class ProbeRecurringJob : IRecurringJob
    {
        public string JobId => "probe";
        public string Cron => "0 0 * * *";
        public Task ExecuteAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class SecondProbeRecurringJob : IRecurringJob
    {
        public string JobId => "probe-2";
        public string Cron => "0 1 * * *";
        public Task ExecuteAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
