using Hangfire;
using Hangfire.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;

namespace Stigar.Workers.Tests;

/// <summary>
/// Pins the Hangfire-facing contract of the Backend polling and snapshot jobs. Their
/// targets must be resolvable by every host sharing the Hangfire store (the API
/// dashboard and the pmtiles scheduler do not load Stigar.Workers.dll). Only the
/// schedule worker consumes the backend-polling queue, so it activates them.
/// </summary>
public class PollingJobRegistrationTests
{
    [Fact]
    public async Task StartAsync_RegistersPollingJobsFromSharedHostingOnDedicatedQueue()
    {
        var manager = new Moq.Mock<IRecurringJobManager>();
        var registrations = new List<(string Id, Job Job, string Cron, RecurringJobOptions Options)>();

        manager
            .Setup(m => m.AddOrUpdate(
                Moq.It.IsAny<string>(),
                Moq.It.IsAny<Job>(),
                Moq.It.IsAny<string>(),
                Moq.It.IsAny<RecurringJobOptions>()))
            .Callback<string, Job, string, RecurringJobOptions>(
                (id, job, cron, options) => registrations.Add((id, job, cron, options)));

        var registration = new PollingJobRegistrationHostedService(
            manager.Object, NullLogger<PollingJobRegistrationHostedService>.Instance);

        await registration.StartAsync(CancellationToken.None);

        Assert.Equal(3, registrations.Count);

        foreach (var (_, job, _, _) in registrations)
        {
            Assert.Equal(typeof(IAdminBoundaryMetricsPollingJob).Assembly, job.Type.Assembly);
            Assert.Equal(AppConfig.BackendPollingQueueName, job.Queue);
        }

        Assert.Contains(registrations, r =>
            r.Job.Type == typeof(IAdminBoundaryMetricsPollingJob) &&
            r.Id == AdminBoundaryMetricsPollingJob.RecurringJobId &&
            r.Cron == AdminBoundaryMetricsPollingJob.CronExpression &&
            r.Job.Method.Name == nameof(IAdminBoundaryMetricsPollingJob.RunAsync));

        Assert.Contains(registrations, r =>
            r.Job.Type == typeof(IQueueActivityJobs) &&
            r.Id == PollingJobRegistrationHostedService.ActivityPollingRecurringJobId &&
            r.Job.Method.Name == nameof(IQueueActivityJobs.PollAndQueueAsync));

        Assert.Contains(registrations, r =>
            r.Job.Type == typeof(IAdminStatusSnapshotJob) &&
            r.Id == AdminStatusSnapshotJob.RecurringJobId &&
            r.Cron == AdminStatusSnapshotJob.CronExpression &&
            r.Job.Method.Name == nameof(IAdminStatusSnapshotJob.RunAsync));
    }
}
