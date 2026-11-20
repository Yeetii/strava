using Hangfire;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;

namespace Stigar.Workers.Pmtiles;

/// <summary>
/// pmtiles-host implementation of the shared Hangfire job contract. Runs one
/// race build per invocation, resolving the build service through a new scope.
/// The job type itself lives in Stigar.Core.Hosting so the API dashboard can enqueue it.
/// </summary>
public sealed class RaceTilesSchedulerJob(
    IServiceProvider services,
    ILogger<RaceTilesSchedulerJob> logger) : IRaceTilesBuildJob
{
    public async Task RunAsync()
    {
        logger.LogInformation("Starting scheduled race PMTiles build.");
        try
        {
            using var scope = services.CreateScope();
            var buildService = scope.ServiceProvider.GetRequiredService<RaceFromOrganizersPmtilesBuildService>();
            await buildService.BuildAsync(writeTransparency: false, CancellationToken.None);
            logger.LogInformation("Scheduled race PMTiles build completed successfully.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Scheduled race PMTiles build failed.");
            throw;
        }
    }
}

/// <summary>
/// Registers the weekly race build as a Hangfire recurring job via the DI-based
/// IRecurringJobManager: the static RecurringJob API needs JobStorage.Current,
/// which a Generic Host never initializes.
/// </summary>
public sealed class RaceTilesRecurringJobRegistrationService(
    IRecurringJobManager recurringJobs,
    IConfiguration configuration,
    ILogger<RaceTilesRecurringJobRegistrationService> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        var cron = configuration[AppConfig.RaceTilesWeeklyCron] ?? AppConfig.DefaultRaceTilesWeeklyCron;
        recurringJobs.AddOrUpdate<IRaceTilesBuildJob>(
            "race-pmtiles-weekly",
            AppConfig.RaceTilesQueueName,
            job => job.RunAsync(),
            cron,
            new RecurringJobOptions
            {
                TimeZone = TimeZoneInfo.Utc,
            });
        logger.LogInformation(
            "Registered weekly race PMTiles build with cron {Cron} on queue {Queue}.",
            cron,
            AppConfig.RaceTilesQueueName);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
