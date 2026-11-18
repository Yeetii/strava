using Npgsql;
using Shared.Services.Postgres;

namespace Shared.Services.Postgres.Admin;

/// <summary>
/// Implements the admin status probes with parameterized SQL. Table identifiers come from the
/// hardcoded <see cref="TableNames"/> whitelist and are quoted; they are never caller-supplied.
/// </summary>
public sealed class AdminStatusRepository(NpgsqlDataSource dataSource) : IAdminStatusRepository
{
    public IReadOnlyList<string> TableNames { get; } =
    [
        "users",
        "sessions",
        "user_sync_items",
        "activities",
        "peaks_groups",
        "peaks",
        "protected_areas",
        "admin_boundaries",
        "osm_tile_queries",
        "activity_summits",
        "activity_visited_paths",
        "activity_visited_areas",
    ];

    public async Task<long> GetTableRowCountAsync(string tableName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(tableName)
            || !TableNames.Contains(tableName, StringComparer.Ordinal))
        {
            throw new ArgumentException($"Unknown table '{tableName}'.", nameof(tableName));
        }

        var quoted = $"\"{tableName.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
        await using var command = dataSource.CreateCommand($"SELECT COUNT(*) FROM {quoted}");
        return await command.ExecuteScalarAsync(cancellationToken) is long count ? count : 0L;
    }

    public async Task<long> GetDatabaseSizeBytesAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("SELECT pg_database_size(current_database())");
        return await command.ExecuteScalarAsync(cancellationToken) is long size ? size : 0L;
    }

    public async Task<int> GetActiveConnectionsAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT COUNT(*)
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
            """);
        var count = await command.ExecuteScalarAsync(cancellationToken) is long value ? value : 0L;
        return (int)Math.Min(count, int.MaxValue);
    }

    public async Task<(int NewActivities, int ProcessedNewActivities)> GetActivityWindowCountsAsync(
        DateTime sinceUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (
                       WHERE processing_status ->> 'summitedPeaks' = 'true'
                         AND processing_status ->> 'visitedPaths' = 'true'
                         AND processing_status ->> 'visitedAreas' = 'true') AS processed
            FROM activities
            WHERE start_date_local >= @sinceUtc
            """);
        command.Parameters.AddValue("sinceUtc", sinceUtc.ToUniversalTime());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return (0, 0);

        return (Clamp(reader.GetInt64(0)), Clamp(reader.GetInt64(1)));
    }

    private static int Clamp(long value) => (int)Math.Min(value, int.MaxValue);
}
