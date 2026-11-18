namespace Shared.Services.Postgres.Admin;

/// <summary>
/// PostgreSQL equivalents of the Cosmos-specific admin status probes. Every method throws on
/// failure so the endpoint can apply the established <c>-1</c>/<c>null</c> error convention.
/// </summary>
public interface IAdminStatusRepository
{
    /// <summary>The twelve tables created by <c>deploy/init-db/01-init.sql</c>.</summary>
    IReadOnlyList<string> TableNames { get; }

    Task<long> GetTableRowCountAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary><c>pg_database_size(current_database())</c> in bytes.</summary>
    Task<long> GetDatabaseSizeBytesAsync(CancellationToken cancellationToken = default);

    /// <summary>Connections to the current database, excluding this probe's own backend.</summary>
    Task<int> GetActiveConnectionsAsync(CancellationToken cancellationToken = default);

    Task<(int NewActivities, int ProcessedNewActivities)> GetActivityWindowCountsAsync(
        DateTime sinceUtc,
        CancellationToken cancellationToken = default);
}
