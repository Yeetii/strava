namespace Stigar.Core.Jobs;

/// <summary>
/// Hourly recurring job that records table row counts and Hangfire queue depths for the
/// admin dashboard trend charts. Runs in the Workers host on the backend polling queue.
/// </summary>
public interface IAdminStatusSnapshotJob
{
    Task RunAsync(CancellationToken cancellationToken = default);
}
