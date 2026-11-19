using Hangfire;
using Hangfire.Common;
using Stigar.Core.Constants;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Stigar.Core.Jobs;

/// <summary>
/// A native Hangfire recurring job. Implementations carry their own cron and
/// stable id, replacing reflection-based timer-trigger discovery.
/// </summary>
public interface IRecurringJob
{
    string JobId { get; }
    string Cron { get; }
    Task ExecuteAsync(CancellationToken cancellationToken);
}

/// <summary>
/// The Hangfire-facing target for every native recurring job. It lives in the
/// shared assembly so any host reading the store can deserialize the job, while
/// the dispatcher implementation stays in the host that executes it. The job id
/// is an argument, so one target serves all crons without a per-job interface.
/// </summary>
public interface IRecurringJobDispatcher
{
    [JobConsoleServerFilter]
    Task RunAsync(string jobId);
}

/// <summary>Registers every IRecurringJob in DI as a Hangfire recurring job.</summary>
public sealed class RecurringJobRegistrationHostedService(
    IEnumerable<IRecurringJob> jobs,
    IRecurringJobManager recurringJobs,
    ILogger<RecurringJobRegistrationHostedService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var method = typeof(IRecurringJobDispatcher).GetMethod(nameof(IRecurringJobDispatcher.RunAsync))!;

        foreach (var job in jobs)
        {
            // Target the shared interface, never the concrete implementation: the
            // API dashboard and the pmtiles scheduler do not load Stigar.Workers.
            // The queue is set on the job so it survives into every invocation;
            // otherwise Hangfire uses "default", which no worker here serves.
            var hangfireJob = new Job(
                typeof(IRecurringJobDispatcher), method, [job.JobId], AppConfig.RecurringJobsQueueName);
            recurringJobs.AddOrUpdate(job.JobId, hangfireJob, job.Cron,
                new RecurringJobOptions { TimeZone = TimeZoneInfo.Utc });
        }

        logger.LogInformation("Registered {Count} native Hangfire recurring jobs", jobs.Count());
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

public static class RecurringJobRegistrationExtensions
{
    public static IHostApplicationBuilder AddRecurringJobs(this IHostApplicationBuilder builder)
    {
        builder.Services.AddSingleton<IHostedService, RecurringJobRegistrationHostedService>();
        return builder;
    }
}
