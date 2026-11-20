namespace Stigar.Core.Services.Postgres.Admin;

public record AdminHistoryPoint(
    DateTime RecordedAtUtc,
    string MetricType,
    string MetricName,
    long Value);

/// <summary>Stores and reads snapshots of admin dashboard metrics.</summary>
public interface IAdminStatusHistoryRepository
{
    /// <summary>Inserts one snapshot. Cadence is owned by the recurring job.</summary>
    Task RecordSnapshotAsync(
        IReadOnlyList<(string Type, string Name, long Value)> metrics,
        CancellationToken cancellationToken = default);

    Task<IReadOnlyList<AdminHistoryPoint>> GetRecentAsync(
        TimeSpan window,
        CancellationToken cancellationToken = default);
}
