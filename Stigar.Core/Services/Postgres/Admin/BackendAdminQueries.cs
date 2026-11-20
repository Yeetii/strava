using BAMCIS.GeoJSON;
using Npgsql;
using NpgsqlTypes;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Core.Services.Postgres.Admin;

/// <summary>
/// ADO.NET implementation of <see cref="IBackendAdminQueries"/>. Uses the shared
/// <see cref="NpgsqlDataSource"/> and value/spatial mapping helpers; no ORM.
/// </summary>
public sealed class BackendAdminQueries(NpgsqlDataSource dataSource) : IBackendAdminQueries
{
    // Rows claimed by a poll are invisible to subsequent polls for this long. Long enough to
    // cover a slow backfill, short enough that a genuinely lost job is eventually retried.
    private const int QueueWindowHours = 24;

    private const string ActivityProjectionsSql = """
        SELECT id, user_id, sport_type, processing_status
        FROM activities
        """;

    private static readonly string ActivityIdsNeedingProcessingSql = $$"""
        SELECT id FROM activities
        WHERE (
                  COALESCE((processing_status->>'summitedPeaksEpoch')::bigint, 0) < @summits_epoch
               OR COALESCE((processing_status->>'visitedPathsEpoch')::bigint, 0) < @paths_epoch
               OR COALESCE((processing_status->>'visitedAreasEpoch')::bigint, 0) < @areas_epoch
              )
          AND COALESCE((processing_status->>'enqueuedAtUtc')::timestamptz, 'epoch')
              < NOW() - INTERVAL '{{QueueWindowHours}} hours'
        ORDER BY id
        LIMIT @limit
        """;

    private static readonly string StaleAdminBoundaryIdsSql = $$"""
        SELECT id FROM admin_boundaries
        WHERE metrics_generation < @epoch
          AND name <> ''
          AND (@after IS NULL OR id > @after)
          AND (metrics_queued_at IS NULL
               OR metrics_queued_at < NOW() - INTERVAL '{{QueueWindowHours}} hours')
        ORDER BY id
        LIMIT @limit
        """;

    private const string ProtectedAreasWithinBoundarySql = """
        SELECT id, properties,
               ST_X(ST_Centroid(geom)) AS cx, ST_Y(ST_Centroid(geom)) AS cy
        FROM protected_areas
        WHERE ST_Intersects(
            ST_Centroid(geom),
            ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326))
        ORDER BY id
        """;

    private const string MissingOsmHighwayIdsSql = """
        SELECT DISTINCT activity_id FROM activity_visited_paths
        WHERE (osm_highway_id IS NULL OR osm_highway_id = '')
          AND (@after IS NULL OR activity_id > @after)
        ORDER BY activity_id
        LIMIT @limit
        """;

    public async Task<IReadOnlyList<BackendActivityProjection>> GetActivityProjections(
        string? userId = null,
        CancellationToken cancellationToken = default)
    {
        var sql = ActivityProjectionsSql;
        if (!string.IsNullOrWhiteSpace(userId))
            sql += " WHERE user_id = @user_id";
        sql += " ORDER BY id";

        await using var command = dataSource.CreateCommand(sql);
        if (!string.IsNullOrWhiteSpace(userId))
            command.Parameters.AddValue("user_id", userId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<BackendActivityProjection>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new BackendActivityProjection(
                reader.GetString(0),
                reader.GetString(1),
                PostgresValueMapping.ReadText(reader, 2),
                PostgresValueMapping.Deserialize<ActivityProcessingStatus>(
                    PostgresValueMapping.ReadJson(reader, 3))));
        }
        return result;
    }

    public async Task<IReadOnlyList<string>> GetActivityIdsNeedingProcessing(
        long summitsEpoch,
        long pathsEpoch,
        long areasEpoch,
        int limit,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(ActivityIdsNeedingProcessingSql);
        command.Parameters.AddValue("summits_epoch", summitsEpoch);
        command.Parameters.AddValue("paths_epoch", pathsEpoch);
        command.Parameters.AddValue("areas_epoch", areasEpoch);
        command.Parameters.AddValue("limit", limit);
        return await ReadStringsAsync(command, cancellationToken);
    }

    public async Task MarkActivitiesEnqueued(
        IReadOnlyCollection<string> activityIds,
        DateTime queuedAtUtc,
        CancellationToken cancellationToken = default)
    {
        if (activityIds.Count == 0)
            return;

        await using var command = dataSource.CreateCommand("""
            UPDATE activities
            SET processing_status = jsonb_set(
                COALESCE(processing_status, '{}'::jsonb),
                '{enqueuedAtUtc}',
                to_jsonb(@queued_at::timestamptz))
            WHERE id = ANY(@ids)
            """);
        AddIds(command, activityIds);
        command.Parameters.AddValue("queued_at", queuedAtUtc);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<string>> GetStaleAdminBoundaryIds(
        long epoch,
        string? afterId,
        int limit,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(StaleAdminBoundaryIdsSql);
        command.Parameters.AddValue("epoch", epoch);
        command.Parameters.AddValue(
            "after", string.IsNullOrWhiteSpace(afterId) ? null : afterId, NpgsqlDbType.Text);
        command.Parameters.AddValue("limit", limit);
        return await ReadStringsAsync(command, cancellationToken);
    }

    public async Task MarkAdminBoundariesQueued(
        IReadOnlyCollection<string> boundaryIds,
        DateTime queuedAtUtc,
        CancellationToken cancellationToken = default)
    {
        if (boundaryIds.Count == 0)
            return;

        await using var command = dataSource.CreateCommand("""
            UPDATE admin_boundaries SET metrics_queued_at = @queued_at WHERE id = ANY(@ids)
            """);
        AddIds(command, boundaryIds);
        command.Parameters.AddValue("queued_at", queuedAtUtc);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<StoredFeatureSummary>> FindProtectedAreaSummariesWithinBoundary(
        Geometry boundary,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(boundary);

        await using var command = dataSource.CreateCommand(ProtectedAreasWithinBoundarySql);
        SpatialRowMapping.AddGeometry(command.Parameters, "geom", boundary);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<StoredFeatureSummary>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(SpatialRowMapping.BuildSummary(
                reader.GetString(0),
                FeatureKinds.ProtectedArea,
                SpatialRowMapping.ReadProperties(reader, 1),
                new Coordinate(reader.GetDouble(2), reader.GetDouble(3)),
                SpatialRowMapping.ProtectedAreaZoom));
        }
        return result;
    }

    public async Task<IReadOnlyList<string>> GetActivityIdsWithMissingOsmHighwayId(
        string? afterActivityId,
        int limit,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(MissingOsmHighwayIdsSql);
        command.Parameters.AddValue(
            "after", string.IsNullOrWhiteSpace(afterActivityId) ? null : afterActivityId, NpgsqlDbType.Text);
        command.Parameters.AddValue("limit", limit);
        return await ReadStringsAsync(command, cancellationToken);
    }

    public async Task<int> RemoveExcludedActivityLinks(
        BackendLinkFeature feature,
        IReadOnlyCollection<string> activityIds,
        bool dryRun,
        CancellationToken cancellationToken = default)
    {
        var ids = (activityIds ?? [])
            .Where(id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (ids.Length == 0)
            return 0;

        // Table names are selected from a fixed set — PostgreSQL parameters cannot bind identifiers.
        var table = feature switch
        {
            BackendLinkFeature.Paths => "activity_visited_paths",
            BackendLinkFeature.Peaks => "activity_summits",
            BackendLinkFeature.Areas => "activity_visited_areas",
            _ => throw new ArgumentOutOfRangeException(nameof(feature), feature, "Unknown link feature."),
        };

        var sql = dryRun
            ? $"SELECT COUNT(*) FROM {table} WHERE activity_id = ANY(@ids)"
            : $"DELETE FROM {table} WHERE activity_id = ANY(@ids)";

        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            Value = ids,
        });

        if (dryRun)
        {
            var count = await command.ExecuteScalarAsync(cancellationToken);
            return count is int intCount ? intCount : Convert.ToInt32(count);
        }

        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddIds(NpgsqlCommand command, IReadOnlyCollection<string> ids)
        => command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
        {
            Value = ids.Distinct(StringComparer.Ordinal).ToArray(),
        });

    private static async Task<IReadOnlyList<string>> ReadStringsAsync(
        NpgsqlCommand command,
        CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(reader.GetString(0));
        return result;
    }
}
