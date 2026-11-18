using Npgsql;
using Shared.Services.Postgres;

namespace Shared.Services.Postgres.Spatial;

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
/// Idempotent access to <c>activity_visited_paths</c>. All writes use <c>ON CONFLICT DO NOTHING</c>.
/// The extra name/type/tile columns are optional and map directly to the DDL.
/// </summary>
public sealed class ActivityVisitedPathRepository(NpgsqlDataSource dataSource) : IActivityVisitedPathRepository
{
    private const string InsertSql = """
        INSERT INTO activity_visited_paths (
            activity_id, user_id, osm_highway_id, name, type, tile_x, tile_y)
        VALUES (
            @activity_id, @user_id, @osm_highway_id, @name, @type, @tile_x, @tile_y)
        ON CONFLICT (activity_id, osm_highway_id) DO NOTHING
        """;

    private const string SelectSql = """
        SELECT activity_id, user_id, osm_highway_id, name, type, tile_x, tile_y, created_at
        FROM activity_visited_paths
        """;

    public Task Add(string activityId, string userId, string osmHighwayId, CancellationToken cancellationToken = default)
        => AddPath(new VisitedPathInput(activityId, userId, osmHighwayId), cancellationToken);

    public Task AddPath(VisitedPathInput path, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        return AddCore(path, cancellationToken);
    }

    public async Task AddRange(IEnumerable<VisitedPathInput> paths, CancellationToken cancellationToken = default)
    {
        var list = (paths ?? []).ToList();
        if (list.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var path in list)
        {
            var command = new NpgsqlBatchCommand(InsertSql);
            AddValues(command.Parameters, path);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Remove(string activityId, string osmHighwayId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM activity_visited_paths WHERE activity_id = @activity_id AND osm_highway_id = @osm_highway_id");
        command.Parameters.AddValue("activity_id", activityId);
        command.Parameters.AddValue("osm_highway_id", osmHighwayId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task RemoveByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM activity_visited_paths WHERE activity_id = @activity_id");
        command.Parameters.AddValue("activity_id", activityId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<VisitedPathLink>> GetByUser(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id ORDER BY activity_id, osm_highway_id");
        command.Parameters.AddValue("user_id", userId);
        return await ReadAll(command, cancellationToken);
    }

    public async Task<IReadOnlyList<VisitedPathLink>> GetByActivity(string activityId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE activity_id = @activity_id ORDER BY osm_highway_id");
        command.Parameters.AddValue("activity_id", activityId);
        return await ReadAll(command, cancellationToken);
    }

    public async Task<int> CountByActivity(string activityId, CancellationToken cancellationToken = default)
        => await Count("SELECT COUNT(*) FROM activity_visited_paths WHERE activity_id = @activity_id", "activity_id", activityId, cancellationToken);

    public async Task<int> CountByUser(string userId, CancellationToken cancellationToken = default)
        => await Count("SELECT COUNT(*) FROM activity_visited_paths WHERE user_id = @user_id", "user_id", userId, cancellationToken);

    private async Task AddCore(VisitedPathInput path, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(InsertSql);
        AddValues(command.Parameters, path);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddValues(NpgsqlParameterCollection parameters, VisitedPathInput path)
    {
        parameters.AddValue("activity_id", path.ActivityId);
        parameters.AddValue("user_id", path.UserId);
        parameters.AddValue("osm_highway_id", path.OsmHighwayId);
        parameters.AddValue("name", path.Name);
        parameters.AddValue("type", path.Type);
        parameters.AddValue("tile_x", path.TileX);
        parameters.AddValue("tile_y", path.TileY);
    }

    private static async Task<IReadOnlyList<VisitedPathLink>> ReadAll(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<VisitedPathLink>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new VisitedPathLink(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                PostgresValueMapping.ReadText(reader, 3),
                PostgresValueMapping.ReadText(reader, 4),
                PostgresValueMapping.ReadInt(reader, 5),
                PostgresValueMapping.ReadInt(reader, 6),
                reader.GetDateTime(7)));
        }
        return result;
    }

    private async Task<int> Count(string sql, string parameter, string value, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddValue(parameter, value);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken));
    }
}
