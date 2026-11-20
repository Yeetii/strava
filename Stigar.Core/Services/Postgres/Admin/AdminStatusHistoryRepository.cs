using Npgsql;

namespace Stigar.Core.Services.Postgres.Admin;

public sealed class AdminStatusHistoryRepository(NpgsqlDataSource dataSource)
    : IAdminStatusHistoryRepository
{
    private const string RecordSql = """
        INSERT INTO admin_status_history (recorded_at, metric_type, metric_name, value)
        SELECT NOW(), m.metric_type, m.metric_name, m.value
        FROM UNNEST(@types::text[], @names::text[], @values::bigint[])
             AS m(metric_type, metric_name, value)
        """;

    private const string RecentSql = """
        SELECT recorded_at, metric_type, metric_name, value
        FROM admin_status_history
        WHERE recorded_at > NOW() - make_interval(secs => @window_seconds)
        ORDER BY recorded_at, metric_type, metric_name
        """;

    public async Task RecordSnapshotAsync(
        IReadOnlyList<(string Type, string Name, long Value)> metrics,
        CancellationToken cancellationToken = default)
    {
        if (metrics.Count == 0)
            return;

        await using var command = dataSource.CreateCommand(RecordSql);
        command.Parameters.AddValue("types", metrics.Select(m => m.Type).ToArray());
        command.Parameters.AddValue("names", metrics.Select(m => m.Name).ToArray());
        command.Parameters.AddValue("values", metrics.Select(m => m.Value).ToArray());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<AdminHistoryPoint>> GetRecentAsync(
        TimeSpan window,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(RecentSql);
        command.Parameters.AddValue("window_seconds", (int)window.TotalSeconds);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<AdminHistoryPoint>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new AdminHistoryPoint(
                reader.GetDateTime(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetInt64(3)));
        }
        return result;
    }
}
