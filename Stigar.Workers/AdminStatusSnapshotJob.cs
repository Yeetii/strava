using Hangfire;
using Microsoft.Extensions.Logging;
using Stigar.Core.Jobs;
using Stigar.Core.Services.Postgres.Admin;

namespace Stigar.Workers;

/// <summary>Records one admin status snapshot per run.</summary>
public sealed class AdminStatusSnapshotJob(
    IAdminStatusRepository status,
    IAdminStatusHistoryRepository history,
    ILogger<AdminStatusSnapshotJob> logger) : IAdminStatusSnapshotJob
{
    public const string RecurringJobId = "record-admin-status-history";
    public const string CronExpression = "0 * * * *";

    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var metrics = new List<(string Type, string Name, long Value)>();

        foreach (var tableName in status.TableNames)
        {
            try
            {
                var (rowCount, _) = await status.GetTableStatsAsync(tableName, cancellationToken);
                if (rowCount >= 0)
                    metrics.Add(("table", tableName, rowCount));
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Skipping table {Table} in status snapshot", tableName);
            }
        }

        var monitoring = JobStorage.Current?.GetMonitoringApi();
        if (monitoring is not null)
        {
            foreach (var queue in monitoring.Queues())
                metrics.Add(("queue", queue.Name, queue.Length));
        }

        await history.RecordSnapshotAsync(metrics, cancellationToken);
        logger.LogInformation(
            "Recorded admin status snapshot with {Count} metrics", metrics.Count);
    }
}
