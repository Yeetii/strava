using BAMCIS.GeoJSON;
using Npgsql;
using NpgsqlTypes;
using Shared.Models;
using Shared.Services.Postgres.Spatial;

namespace Shared.Services.Postgres.Admin;

/// <summary>
/// ADO.NET implementation of <see cref="IBackendAdminQueries"/>. Uses the shared
/// <see cref="NpgsqlDataSource"/> and value/spatial mapping helpers; no ORM.
/// </summary>
public sealed class BackendAdminQueries(NpgsqlDataSource dataSource) : IBackendAdminQueries
{
    private const string ActivityProjectionsSql = """
        SELECT id, user_id, sport_type, processing_status
        FROM activities
        """;

    private const string ActivityIdsNeedingProcessingSql = """
        SELECT id FROM activities
        WHERE processing_status IS NULL
           OR COALESCE((processing_status->>'summitedPeaks')::boolean, false) = false
           OR COALESCE((processing_status->>'visitedPaths')::boolean, false) = false
           OR COALESCE((processing_status->>'visitedAreas')::boolean, false) = false
        ORDER BY id
        LIMIT @limit
        """;

    private const string StaleAdminBoundaryIdsSql = """
        SELECT id FROM admin_boundaries
        WHERE metrics_version < @version
          AND name <> ''
          AND (@after IS NULL OR id > @after)
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
        int limit,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(ActivityIdsNeedingProcessingSql);
        command.Parameters.AddValue("limit", limit);
        return await ReadStringsAsync(command, cancellationToken);
    }

    public async Task<IReadOnlyList<string>> GetStaleAdminBoundaryIds(
        int metricsVersion,
        string? afterId,
        int limit,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(StaleAdminBoundaryIdsSql);
        command.Parameters.AddValue("version", metricsVersion);
        command.Parameters.AddValue("after", string.IsNullOrWhiteSpace(afterId) ? null : afterId);
        command.Parameters.AddValue("limit", limit);
        return await ReadStringsAsync(command, cancellationToken);
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
        command.Parameters.AddValue("after", string.IsNullOrWhiteSpace(afterActivityId) ? null : afterActivityId);
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
