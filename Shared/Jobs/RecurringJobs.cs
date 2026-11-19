using Hangfire;
using Hangfire.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Shared.Jobs;

/// <summary>
/// A native Hangfire recurring job. Implementations carry their own cron and
/// stable id, replacing the reflection-based [TimerTrigger] discovery.
/// </summary>
public interface IRecurringJob
{
    string JobId { get; }
    string Cron { get; }
    Task ExecuteAsync(CancellationToken cancellationToken);
}

/// <summary>Registers every IRecurringJob in DI as a Hangfire recurring job.</summary>
public sealed class RecurringJobRegistrationHostedService(
    IEnumerable<IRecurringJob> jobs,
    IRecurringJobManager recurringJobs,
    ILogger<RecurringJobRegistrationHostedService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var job in jobs)
        {
            var jobType = job.GetType();
            var method = jobType.GetMethod(nameof(IRecurringJob.ExecuteAsync))!;
            recurringJobs.AddOrUpdate(
                job.JobId,
                new Job(jobType, method, []),
                job.Cron,
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
