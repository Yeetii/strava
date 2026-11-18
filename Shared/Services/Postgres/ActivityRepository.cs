using Npgsql;
using NpgsqlTypes;
using Shared.Models;

namespace Shared.Services.Postgres;

/// <summary>
/// Reads and writes the <c>activities</c> table. <c>processing_status</c> is JSONB mapped to
/// <see cref="ActivityProcessingStatus"/>; <c>start_lat_lng</c>/<c>end_lat_lng</c> are REAL[]
/// mapped to <see cref="float"/> arrays. The Cosmos-only <see cref="Activity.ETag"/> is not persisted.
/// </summary>
public sealed class ActivityRepository(NpgsqlDataSource dataSource) : IActivityRepository
{
    private const string SelectSql = """
        SELECT id, user_id, name, description, sport_type, start_date, start_date_local,
               timezone, start_lat_lng, end_lat_lng, distance, moving_time, elapsed_time,
               calories, total_elevation_gain, elev_high, elev_low, average_speed, max_speed,
               athlete_count, polyline, summary_polyline, processing_status
        FROM activities
        """;

    private const string UpsertSql = """
        INSERT INTO activities (
            id, user_id, name, description, sport_type, start_date, start_date_local,
            timezone, start_lat_lng, end_lat_lng, distance, moving_time, elapsed_time,
            calories, total_elevation_gain, elev_high, elev_low, average_speed, max_speed,
            athlete_count, polyline, summary_polyline, processing_status
        ) VALUES (
            @id, @user_id, @name, @description, @sport_type, @start_date, @start_date_local,
            @timezone, @start_lat_lng, @end_lat_lng, @distance, @moving_time, @elapsed_time,
            @calories, @total_elevation_gain, @elev_high, @elev_low, @average_speed, @max_speed,
            @athlete_count, @polyline, @summary_polyline, @processing_status
        )
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            sport_type = EXCLUDED.sport_type,
            start_date = EXCLUDED.start_date,
            start_date_local = EXCLUDED.start_date_local,
            timezone = EXCLUDED.timezone,
            start_lat_lng = EXCLUDED.start_lat_lng,
            end_lat_lng = EXCLUDED.end_lat_lng,
            distance = EXCLUDED.distance,
            moving_time = EXCLUDED.moving_time,
            elapsed_time = EXCLUDED.elapsed_time,
            calories = EXCLUDED.calories,
            total_elevation_gain = EXCLUDED.total_elevation_gain,
            elev_high = EXCLUDED.elev_high,
            elev_low = EXCLUDED.elev_low,
            average_speed = EXCLUDED.average_speed,
            max_speed = EXCLUDED.max_speed,
            athlete_count = EXCLUDED.athlete_count,
            polyline = EXCLUDED.polyline,
            summary_polyline = EXCLUDED.summary_polyline,
            processing_status = EXCLUDED.processing_status
        """;

    public async Task<Activity?> GetById(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE id = @id LIMIT 1");
        command.Parameters.AddValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? Map(reader) : null;
    }

    public async Task<IReadOnlyList<Activity>> GetByIds(IEnumerable<string> ids, CancellationToken cancellationToken = default)
    {
        var idArray = (ids ?? [])
            .Where(id => !string.IsNullOrEmpty(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (idArray.Length == 0)
            return [];

        await using var command = dataSource.CreateCommand(SelectSql + " WHERE id = ANY(@ids) ORDER BY id");
        command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            Value = idArray,
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var activities = new List<Activity>();
        while (await reader.ReadAsync(cancellationToken))
            activities.Add(Map(reader));
        return activities;
    }

    public async Task<IReadOnlyList<Activity>> GetAllByUser(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id ORDER BY start_date_local DESC, id");
        command.Parameters.AddValue("user_id", userId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var activities = new List<Activity>();
        while (await reader.ReadAsync(cancellationToken))
            activities.Add(Map(reader));
        return activities;
    }

    public async Task<IReadOnlyList<string>> GetAllIds(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("SELECT id FROM activities ORDER BY id");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var ids = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
            ids.Add(reader.GetString(0));
        return ids;
    }

    public async Task Upsert(Activity activity, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(activity);
        await using var command = dataSource.CreateCommand(UpsertSql);
        AddParameters(command.Parameters, activity);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// One batched round trip for the whole collection; each row uses the same id-based upsert.
    /// </summary>
    public async Task BulkUpsert(IEnumerable<Activity> activities, CancellationToken cancellationToken = default)
    {
        var list = (activities ?? []).ToList();
        if (list.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var activity in list)
        {
            ArgumentNullException.ThrowIfNull(activity);
            var command = new NpgsqlBatchCommand(UpsertSql);
            AddParameters(command.Parameters, activity);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM activities WHERE id = @id");
        command.Parameters.AddValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateProcessingStatus(
        string id,
        ActivityProcessingStatus? status,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "UPDATE activities SET processing_status = @processing_status WHERE id = @id");
        command.Parameters.AddValue("id", id);
        command.Parameters.AddJsonb("processing_status", PostgresValueMapping.Serialize(status));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, Activity activity)
    {
        parameters.AddValue("id", activity.Id);
        parameters.AddValue("user_id", activity.UserId);
        parameters.AddValue("name", activity.Name);
        parameters.AddValue("description", activity.Description);
        parameters.AddValue("sport_type", activity.SportType);
        parameters.AddValue("start_date", PostgresValueMapping.AsUtc(activity.StartDate));
        parameters.AddValue("start_date_local", PostgresValueMapping.AsUtc(activity.StartDateLocal));
        parameters.AddValue("timezone", activity.Timezone);
        parameters.AddRealArray("start_lat_lng", activity.StartLatLng);
        parameters.AddRealArray("end_lat_lng", activity.EndLatLng);
        parameters.AddValue("distance", activity.Distance);
        parameters.AddValue("moving_time", activity.MovingTime);
        parameters.AddValue("elapsed_time", activity.ElapsedTime);
        parameters.AddValue("calories", activity.Calories);
        parameters.AddValue("total_elevation_gain", activity.TotalElevationGain);
        parameters.AddValue("elev_high", activity.ElevHigh);
        parameters.AddValue("elev_low", activity.ElevLow);
        parameters.AddValue("average_speed", activity.AverageSpeed);
        parameters.AddValue("max_speed", activity.MaxSpeed);
        parameters.AddValue("athlete_count", activity.AthleteCount);
        parameters.AddValue("polyline", activity.Polyline);
        parameters.AddValue("summary_polyline", activity.SummaryPolyline);
        parameters.AddJsonb("processing_status", PostgresValueMapping.Serialize(activity.ProcessingStatus));
    }

    private static Activity Map(NpgsqlDataReader reader) => new()
    {
        Id = reader.GetString(0),
        UserId = reader.GetString(1),
        Name = reader.GetString(2),
        Description = PostgresValueMapping.ReadText(reader, 3),
        SportType = reader.GetString(4),
        StartDate = reader.GetDateTime(5),
        StartDateLocal = reader.GetDateTime(6),
        Timezone = PostgresValueMapping.ReadText(reader, 7),
        StartLatLng = PostgresValueMapping.ReadRealArray(reader, 8),
        EndLatLng = PostgresValueMapping.ReadRealArray(reader, 9),
        Distance = PostgresValueMapping.ReadFloat(reader, 10),
        MovingTime = PostgresValueMapping.ReadFloat(reader, 11),
        ElapsedTime = PostgresValueMapping.ReadFloat(reader, 12),
        Calories = PostgresValueMapping.ReadFloat(reader, 13),
        TotalElevationGain = PostgresValueMapping.ReadFloat(reader, 14),
        ElevHigh = PostgresValueMapping.ReadFloat(reader, 15),
        ElevLow = PostgresValueMapping.ReadFloat(reader, 16),
        AverageSpeed = PostgresValueMapping.ReadFloat(reader, 17),
        MaxSpeed = PostgresValueMapping.ReadFloat(reader, 18),
        AthleteCount = PostgresValueMapping.ReadInt(reader, 19),
        Polyline = PostgresValueMapping.ReadText(reader, 20),
        SummaryPolyline = PostgresValueMapping.ReadText(reader, 21),
        ProcessingStatus = PostgresValueMapping.Deserialize<ActivityProcessingStatus>(
            PostgresValueMapping.ReadJson(reader, 22)),
    };
}
