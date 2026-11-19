namespace Stigar.Core.Jobs;

/// <summary>
/// Hangfire-facing contract for the admin-boundary metrics polling job.
///
/// This interface deliberately lives in Stigar.Core.Hosting, which every host sharing
/// the Hangfire store loads. Keeping the target as the concrete Backend type makes
/// other hosts fail with "Could not resolve assembly 'Backend'" while deserializing
/// the recurring job (the API dashboard and the pmtiles scheduler both hit this).
/// Only the heavy worker supplies the implementation; other hosts only need the type.
/// </summary>
public interface IAdminBoundaryMetricsPollingJob
{
    [JobConsoleServerFilter]
    Task RunAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Hangfire-facing contract for the activity-processing polling job. Lives in
/// Stigar.Core.Hosting for the same reason as <see cref="IAdminBoundaryMetricsPollingJob"/>.
/// </summary>
public interface IQueueActivityJobs
{
    [JobConsoleServerFilter]
    Task PollAndQueueAsync(CancellationToken cancellationToken = default);
}
