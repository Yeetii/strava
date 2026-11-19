using Hangfire;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Jobs;

namespace Backend;

/// <summary>
/// Registers the two PostgreSQL polling jobs as Hangfire recurring jobs on the
/// heavy worker's dedicated queue. The targets are interfaces in Shared.Hosting
/// so every host sharing the Hangfire store can deserialize them, while the
/// queue keeps activation on the heavy worker that registered the implementations.
/// </summary>
public sealed class PollingJobRegistrationHostedService(
    IRecurringJobManager recurringJobs,
    ILogger<PollingJobRegistrationHostedService> logger) : IHostedService
{
    public const string ActivityPollingRecurringJobId = "poll-activity-processing-jobs";
    public const string ActivityPollingCron = "*/5 * * * *";

    public Task StartAsync(CancellationToken cancellationToken)
    {
        recurringJobs.AddOrUpdate<IAdminBoundaryMetricsPollingJob>(
            AdminBoundaryMetricsPollingJob.RecurringJobId,
            AppConfig.BackendPollingQueueName,
            job => job.RunAsync(CancellationToken.None),
            () => AdminBoundaryMetricsPollingJob.CronExpression,
            PollingOptions());

        recurringJobs.AddOrUpdate<IQueueActivityJobs>(
            ActivityPollingRecurringJobId,
            AppConfig.BackendPollingQueueName,
            job => job.PollAndQueueAsync(CancellationToken.None),
            () => ActivityPollingCron,
            PollingOptions());

        logger.LogInformation(
            "Registered admin-boundary and activity polling jobs on Hangfire queue {Queue}",
            AppConfig.BackendPollingQueueName);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private static RecurringJobOptions PollingOptions() => new()
    {
        TimeZone = TimeZoneInfo.Utc,
    };
}
