using Hangfire;
using Hangfire.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Stigar.Core.Jobs;

namespace Stigar.Workers.Tests;

public class RecurringJobRegistrationTests
{
    [Fact]
    public async Task StartAsync_RegistersJobWithArgumentMatchingExecuteAsync()
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
        Assert.Equal(typeof(ProbeRecurringJob), captured!.Type);
        Assert.Single(captured.Arguments);
    }

    private sealed class ProbeRecurringJob : IRecurringJob
    {
        public string JobId => "probe";
        public string Cron => "0 0 * * *";
        public Task ExecuteAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
