using Npgsql;
using NpgsqlTypes;
using Stigar.Core.Services.Postgres;

namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>
/// Idempotent access to <c>activity_summits</c>. All writes use <c>ON CONFLICT DO NOTHING</c>;
/// the FK cascade to activities/users/peaks comes from the DDL.
/// </summary>
public sealed class ActivitySummitRepository(NpgsqlDataSource dataSource) : IActivitySummitRepository
{
    private const string InsertSql = """
        INSERT INTO activity_summits (activity_id, user_id, peak_id)
        VALUES (@activity_id, @user_id, @peak_id)
        ON CONFLICT (activity_id, peak_id) DO NOTHING
        """;

    private const string SelectSql = """
        SELECT activity_id, user_id, peak_id, created_at
        FROM activity_summits
        """;

    public async Task Add(string activityId, string userId, string peakId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(InsertSql);
        AddKey(command.Parameters, activityId, userId, peakId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task AddRange(IEnumerable<SummitLink> links, CancellationToken cancellationToken = default)
    {
        var list = (links ?? []).ToList();
        if (list.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var link in list)
        {
            var command = new NpgsqlBatchCommand(InsertSql);
            AddKey(command.Parameters, link.ActivityId, link.UserId, link.PeakId);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Remove(string activityId, string peakId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM activity_summits WHERE activity_id = @activity_id AND peak_id = @peak_id");
        command.Parameters.AddValue("activity_id", activityId);
        command.Parameters.AddValue("peak_id", peakId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM activity_summits WHERE activity_id = @activity_id");
        command.Parameters.AddValue("activity_id", activityId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<SummitLink>> GetByUser(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id ORDER BY activity_id, peak_id");
        command.Parameters.AddValue("user_id", userId);
        return await ReadAll(command, cancellationToken);
    }

    public async Task<IReadOnlyList<SummitLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE activity_id = @activity_id ORDER BY peak_id");
        command.Parameters.AddValue("activity_id", activityId);
        return await ReadAll(command, cancellationToken);
    }

    public async Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default)
        => await Count("SELECT COUNT(*) FROM activity_summits WHERE activity_id = @activity_id", "activity_id", activityId, cancellationToken);

    public async Task<int> CountByUser(string userId, CancellationToken cancellationToken = default)
        => await Count("SELECT COUNT(*) FROM activity_summits WHERE user_id = @user_id", "user_id", userId, cancellationToken);

    private static async Task<IReadOnlyList<SummitLink>> ReadAll(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<SummitLink>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(new SummitLink(
                reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetDateTime(3)));
        return result;
    }

    private async Task<int> Count(string sql, string parameter, string value, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddValue(parameter, value);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
    }

    private static void AddKey(NpgsqlParameterCollection parameters, string activityId, string userId, string peakId)
    {
        parameters.AddValue("activity_id", activityId);
        parameters.AddValue("user_id", userId);
        parameters.AddValue("peak_id", peakId);
    }
}

/// <summary>
/// Idempotent access to <c>activity_visited_areas</c>. All writes use <c>ON CONFLICT DO NOTHING</c>.
/// </summary>
public sealed class ActivityVisitedAreaRepository(NpgsqlDataSource dataSource) : IActivityVisitedAreaRepository
{
    private const string InsertSql = """
        INSERT INTO activity_visited_areas (activity_id, user_id, area_id)
        VALUES (@activity_id, @user_id, @area_id)
        ON CONFLICT (activity_id, area_id) DO NOTHING
        """;

    private const string SelectSql = """
        SELECT activity_id, user_id, area_id, created_at
        FROM activity_visited_areas
        """;

    public async Task Add(string activityId, string userId, string areaId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(InsertSql);
        AddKey(command.Parameters, activityId, userId, areaId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task AddRange(IEnumerable<VisitedAreaLink> links, CancellationToken cancellationToken = default)
    {
        var list = (links ?? []).ToList();
        if (list.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var link in list)
        {
            var command = new NpgsqlBatchCommand(InsertSql);
            AddKey(command.Parameters, link.ActivityId, link.UserId, link.AreaId);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Remove(string activityId, string areaId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM activity_visited_areas WHERE activity_id = @activity_id AND area_id = @area_id");
        command.Parameters.AddValue("activity_id", activityId);
        command.Parameters.AddValue("area_id", areaId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM activity_visited_areas WHERE activity_id = @activity_id");
        command.Parameters.AddValue("activity_id", activityId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<VisitedAreaLink>> GetByUser(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id ORDER BY activity_id, area_id");
        command.Parameters.AddValue("user_id", userId);
        return await ReadAll(command, cancellationToken);
    }

    public async Task<IReadOnlyList<VisitedAreaLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE activity_id = @activity_id ORDER BY area_id");
        command.Parameters.AddValue("activity_id", activityId);
        return await ReadAll(command, cancellationToken);
    }

    public async Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default)
        => await Count("SELECT COUNT(*) FROM activity_visited_areas WHERE activity_id = @activity_id", "activity_id", activityId, cancellationToken);

    public async Task<int> CountByUser(string userId, CancellationToken cancellationToken = default)
        => await Count("SELECT COUNT(*) FROM activity_visited_areas WHERE user_id = @user_id", "user_id", userId, cancellationToken);

    private static async Task<IReadOnlyList<VisitedAreaLink>> ReadAll(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<VisitedAreaLink>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(new VisitedAreaLink(
                reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetDateTime(3)));
        return result;
    }

    private async Task<int> Count(string sql, string parameter, string value, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddValue(parameter, value);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
    }

    private static void AddKey(NpgsqlParameterCollection parameters, string activityId, string userId, string areaId)
    {
        parameters.AddValue("activity_id", activityId);
        parameters.AddValue("user_id", userId);
        parameters.AddValue("area_id", areaId);
    }
}

/// <summary>
/// Idempotent access to <c>activity_visited_tiles</c>. Writes are grouped by
/// (activity, tile) and use <c>ON CONFLICT DO NOTHING</c>.
/// </summary>
public sealed class ActivityVisitedPathRepository(NpgsqlDataSource dataSource) : IActivityVisitedPathRepository
{
    private const string InsertSql = """
        INSERT INTO activity_visited_tiles (
            activity_id, user_id, tile_x, tile_y, highway_ids)
        VALUES (
            @activity_id, @user_id, @tile_x, @tile_y, @highway_ids)
        ON CONFLICT (activity_id, tile_x, tile_y) DO NOTHING
        """;

    private const string SelectSql = """
        SELECT activity_id, user_id, tile_x, tile_y, highway_ids
        FROM activity_visited_tiles
        """;

    private const string SummarySql = """
        SELECT h.highway_id,
               COUNT(DISTINCT t.activity_id)::int AS times_visited,
               MIN(a.start_date_local) AS first_date,
               MAX(a.start_date_local) AS last_date
        FROM activity_visited_tiles t
        CROSS JOIN LATERAL unnest(t.highway_ids) AS h(highway_id)
        JOIN activities a ON a.id = t.activity_id
        WHERE t.user_id = @user_id
          AND (t.tile_x, t.tile_y) IN ({0})
        GROUP BY h.highway_id
        ORDER BY times_visited DESC
        """;

    public Task Add(string activityId, string userId, string osmHighwayId, int tileX, int tileY, CancellationToken cancellationToken = default)
        => AddCore(new VisitedPathInput(activityId, userId, osmHighwayId, tileX, tileY), cancellationToken);

    public Task AddPath(VisitedPathInput path, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        return AddCore(path, cancellationToken);
    }

    public async Task AddRange(IEnumerable<VisitedPathInput> paths, CancellationToken cancellationToken = default)
    {
        var groups = (paths ?? [])
            .GroupBy(p => (p.ActivityId, p.UserId, p.TileX, p.TileY))
            .Select(g => new
            {
                g.Key.ActivityId,
                g.Key.UserId,
                g.Key.TileX,
                g.Key.TileY,
                HighwayIds = g.Select(p => p.OsmHighwayId).Distinct().OrderBy(id => id).ToArray(),
            })
            .ToList();
        if (groups.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var group in groups)
        {
            var command = new NpgsqlBatchCommand(InsertSql);
            AddValues(command.Parameters, group.ActivityId, group.UserId, group.TileX, group.TileY, group.HighwayIds);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM activity_visited_tiles WHERE activity_id = @activity_id");
        command.Parameters.AddValue("activity_id", activityId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<VisitedTileLink>> GetByUser(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id ORDER BY activity_id, tile_x, tile_y");
        command.Parameters.AddValue("user_id", userId);
        return await ReadAll(command, cancellationToken);
    }

    public async Task<IReadOnlyList<VisitedTileLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE activity_id = @activity_id ORDER BY tile_x, tile_y");
        command.Parameters.AddValue("activity_id", activityId);
        return await ReadAll(command, cancellationToken);
    }

    public Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default)
        => CountSum("SELECT COALESCE(SUM(cardinality(highway_ids)), 0)::int FROM activity_visited_tiles WHERE activity_id = @activity_id",
            "activity_id", activityId, cancellationToken);

    public Task<int> CountByUser(string userId, CancellationToken cancellationToken = default)
        => CountSum("SELECT COALESCE(SUM(cardinality(highway_ids)), 0)::int FROM activity_visited_tiles WHERE user_id = @user_id",
            "user_id", userId, cancellationToken);

    public async Task<IReadOnlyList<VisitedPathSummary>> GetSummaryByUserAndTiles(
        string userId, IReadOnlyList<(int x, int y)> tiles, CancellationToken cancellationToken = default)
    {
        if (tiles is null || tiles.Count == 0)
            return [];

        // Tile coordinates are ints produced by the caller, never user input.
        var pairs = string.Join(", ", tiles.Select(t => $"({t.x}, {t.y})"));
        await using var command = dataSource.CreateCommand(SummarySql.Replace("{0}", pairs));
        command.Parameters.AddValue("user_id", userId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<VisitedPathSummary>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new VisitedPathSummary(
                reader.GetString(0),
                reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3)));
        }
        return result;
    }

    private async Task AddCore(VisitedPathInput path, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(InsertSql);
        AddValues(command.Parameters, path.ActivityId, path.UserId, path.TileX, path.TileY, [path.OsmHighwayId]);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddValues(
        NpgsqlParameterCollection parameters,
        string activityId, string userId, int tileX, int tileY, string[] highwayIds)
    {
        parameters.AddValue("activity_id", activityId);
        parameters.AddValue("user_id", userId);
        parameters.AddValue("tile_x", tileX);
        parameters.AddValue("tile_y", tileY);
        parameters.Add(new NpgsqlParameter("highway_ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            Value = highwayIds,
        });
    }

    private static async Task<IReadOnlyList<VisitedTileLink>> ReadAll(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<VisitedTileLink>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new VisitedTileLink(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetInt32(2),
                reader.GetInt32(3),
                reader.GetFieldValue<string[]>(4)));
        }
        return result;
    }

    private async Task<int> CountSum(string sql, string parameter, string value, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddValue(parameter, value);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
    }
}
