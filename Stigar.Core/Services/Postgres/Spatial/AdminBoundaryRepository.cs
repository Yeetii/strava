using System.Globalization;
using BAMCIS.GeoJSON;
using Npgsql;
using NpgsqlTypes;
using Stigar.Core.Geo;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;

namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>
/// Reads and writes the <c>admin_boundaries</c> table. <c>geom_simplified</c> is a nullable geometry
/// and <c>properties</c> is JSONB. There are no tile columns, so tile lookups use a WGS84 envelope
/// derived from <see cref="SlippyTileCalculator.TileIndexToWGS84"/> and the geometry GIST index.
/// </summary>
public sealed class AdminBoundaryRepository(NpgsqlDataSource dataSource) : IAdminBoundaryRepository
{
    private const string SelectSql = """
        SELECT id, osm_id, osm_type, admin_level, name, wikidata,
               geom_simplified IS NOT NULL AS has_simplified, properties, metrics_version,
               ST_AsGeoJSON(geom) AS geom_json
        FROM admin_boundaries
        """;

    private const string UpsertSql = """
        INSERT INTO admin_boundaries (
            id, osm_id, osm_type, admin_level, name, wikidata, geom, geom_simplified,
            properties, metrics_version)
        VALUES (
            @id, @osm_id, @osm_type, @admin_level, @name, @wikidata,
            ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326),
            CASE WHEN @geom_simplified IS NULL THEN NULL
                 ELSE ST_SetSRID(ST_GeomFromGeoJSON(@geom_simplified), 4326) END,
            @properties, @metrics_version)
        ON CONFLICT (id) DO UPDATE SET
            osm_id = EXCLUDED.osm_id,
            osm_type = EXCLUDED.osm_type,
            admin_level = EXCLUDED.admin_level,
            name = EXCLUDED.name,
            wikidata = EXCLUDED.wikidata,
            geom = EXCLUDED.geom,
            geom_simplified = EXCLUDED.geom_simplified,
            properties = EXCLUDED.properties,
            metrics_version = EXCLUDED.metrics_version
        """;

    private const string ContainingAnySummarySql = """
        SELECT DISTINCT ab.id, ab.properties,
               ST_X(ST_Centroid(ab.geom)) AS cx, ST_Y(ST_Centroid(ab.geom)) AS cy
        FROM admin_boundaries ab
        WHERE (@admin_level IS NULL OR ab.admin_level = @admin_level)
          AND EXISTS (
              SELECT 1 FROM unnest(@lngs::float8[], @lats::float8[]) AS q(lng, lat)
              WHERE ST_Intersects(ab.geom, ST_SetSRID(ST_MakePoint(q.lng, q.lat), 4326)))
        """;

    public async Task<StoredFeature?> GetById(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE id = @id LIMIT 1");
        command.Parameters.AddValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? Map(reader) : null;
    }

    public async Task<IReadOnlyList<StoredFeature>> GetByIds(IEnumerable<string> ids, CancellationToken cancellationToken = default)
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
        var result = new List<StoredFeature>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(Map(reader));
        return result;
    }

    public async Task<IReadOnlyList<StoredFeature>> GetByLevel(int adminLevel, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE admin_level = @admin_level ORDER BY id");
        command.Parameters.AddValue("admin_level", adminLevel);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<StoredFeature>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(Map(reader));
        return result;
    }

    public async Task<IReadOnlyList<StoredFeature>> GetByTile(
        int x,
        int y,
        int adminLevel,
        int zoom,
        CancellationToken cancellationToken = default)
    {
        var (southWest, northEast) = SlippyTileCalculator.TileIndexToWGS84(x, y, zoom);

        await using var command = dataSource.CreateCommand(SelectSql + """
             WHERE admin_level = @admin_level
               AND ST_Intersects(geom, ST_MakeEnvelope(@min_lng, @min_lat, @max_lng, @max_lat, 4326))
            ORDER BY id
            """);
        command.Parameters.AddValue("admin_level", adminLevel);
        command.Parameters.AddValue("min_lng", southWest.Lng);
        command.Parameters.AddValue("min_lat", southWest.Lat);
        command.Parameters.AddValue("max_lng", northEast.Lng);
        command.Parameters.AddValue("max_lat", northEast.Lat);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<StoredFeature>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(Map(reader));
        return result;
    }

    public async Task<IReadOnlyList<StoredFeatureSummary>> FindSummariesContainingAnyPoint(
        IEnumerable<Coordinate> points,
        int? adminLevel = null,
        CancellationToken cancellationToken = default)
    {
        var distinct = (points ?? [])
            .Where(point => point is not null)
            .DistinctBy(point => (point.Lng, point.Lat))
            .ToArray();
        if (distinct.Length == 0)
            return [];

        await using var command = dataSource.CreateCommand(ContainingAnySummarySql);
        command.Parameters.AddValue("admin_level", adminLevel);
        command.Parameters.Add(new NpgsqlParameter("lngs", NpgsqlDbType.Array | NpgsqlDbType.Double)
        {
            Value = distinct.Select(point => point.Lng).ToArray(),
        });
        command.Parameters.Add(new NpgsqlParameter("lats", NpgsqlDbType.Array | NpgsqlDbType.Double)
        {
            Value = distinct.Select(point => point.Lat).ToArray(),
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<StoredFeatureSummary>();
        while (await reader.ReadAsync(cancellationToken))
        {
            var id = reader.GetString(0);
            result.Add(SpatialRowMapping.BuildSummary(
                id,
                FeatureKinds.AdminBoundary,
                SpatialRowMapping.ReadProperties(reader, 1),
                new Coordinate(reader.GetDouble(2), reader.GetDouble(3)),
                SpatialRowMapping.AdminBoundaryZoom));
        }
        return result;
    }

    public async Task Upsert(StoredFeature boundary, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(boundary);
        await using var command = dataSource.CreateCommand(UpsertSql);
        AddParameters(command.Parameters, boundary);
        SpatialRowMapping.AddNullableGeometry(command.Parameters, "geom_simplified", null);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task BulkUpsert(IEnumerable<StoredFeature> boundaries, CancellationToken cancellationToken = default)
    {
        var list = (boundaries ?? []).ToList();
        if (list.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var boundary in list)
        {
            ArgumentNullException.ThrowIfNull(boundary);
            var command = new NpgsqlBatchCommand(UpsertSql);
            AddParameters(command.Parameters, boundary);
            SpatialRowMapping.AddNullableGeometry(command.Parameters, "geom_simplified", null);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task SetSimplifiedGeometry(string id, Geometry simplified, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(simplified);
        await using var command = dataSource.CreateCommand("""
            UPDATE admin_boundaries
            SET geom_simplified = ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326)
            WHERE id = @id
            """);
        command.Parameters.AddValue("id", id);
        SpatialRowMapping.AddGeometry(command.Parameters, "geom", simplified);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task UpdateMetrics(
        string id,
        IReadOnlyDictionary<string, object?> metrics,
        long epoch,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(metrics);
        await using var command = dataSource.CreateCommand("""
            UPDATE admin_boundaries
            SET properties = properties || @metrics::jsonb,
                metrics_generation = @epoch,
                metrics_queued_at = NULL
            WHERE id = @id
            """);
        command.Parameters.AddValue("id", id);
        command.Parameters.AddJsonb("metrics", SpatialRowMapping.ObjectToJson(metrics));
        command.Parameters.AddValue("epoch", epoch);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> DeleteAll(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM admin_boundaries");
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM admin_boundaries WHERE id = @id");
        command.Parameters.AddValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, StoredFeature boundary)
    {
        var properties = boundary.Properties;
        var (osmId, osmType) = GetOsmIdentity(boundary);

        parameters.AddValue("id", boundary.Id);
        parameters.AddValue("osm_id", osmId);
        parameters.AddValue("osm_type", osmType);
        parameters.AddValue("admin_level", SpatialRowMapping.GetInt(properties, "adminLevel") ?? 0);
        parameters.AddValue("name", SpatialRowMapping.GetText(properties, "name") ?? string.Empty);
        parameters.AddValue("wikidata", SpatialRowMapping.GetText(properties, "wikidata"));
        parameters.AddJsonb("properties", SpatialRowMapping.PropertiesToJson(properties));
        parameters.AddValue("metrics_version", SpatialRowMapping.GetInt(properties, "adminBoundaryMetricsVersion") ?? 0);
        SpatialRowMapping.AddGeometry(parameters, "geom", boundary.Geometry);
    }

    private static (long osmId, string osmType) GetOsmIdentity(StoredFeature boundary)
    {
        var properties = boundary.Properties;
        var osmType = SpatialRowMapping.GetText(properties, "osmType");
        var osmId = SpatialRowMapping.GetLong(properties, "osmId");

        var featureId = boundary.FeatureId
            ?? StoredFeature.NormalizeFeatureId(FeatureKinds.AdminBoundary, boundary.Id);
        if (!string.IsNullOrWhiteSpace(featureId))
        {
            var separator = featureId.LastIndexOf(':');
            var prefix = separator > 0 ? featureId[..featureId.IndexOf(':')] : string.Empty;
            var numeric = separator > 0 ? featureId[(separator + 1)..] : featureId;

            if (prefix is "relation" or "way" or "node")
                osmType ??= prefix;
            if (!osmId.HasValue && long.TryParse(numeric, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
                osmId = parsed;
        }

        return (osmId ?? 0, osmType ?? "relation");
    }

    private static StoredFeature Map(NpgsqlDataReader reader)
    {
        var id = reader.GetString(0);
        var properties = SpatialRowMapping.ReadProperties(reader, 7);
        Overlay(properties, "name", PostgresValueMapping.ReadText(reader, 4));
        Overlay(properties, "wikidata", PostgresValueMapping.ReadText(reader, 5));
        Overlay(properties, "adminLevel", reader.GetInt32(3).ToString(CultureInfo.InvariantCulture));
        Overlay(properties, "osmType", PostgresValueMapping.ReadText(reader, 2));
        Overlay(properties, "osmId", reader.GetInt64(1).ToString(CultureInfo.InvariantCulture));
        Overlay(properties, "metricsVersion", reader.GetInt32(8).ToString(CultureInfo.InvariantCulture));

        var geometry = SpatialRowMapping.ReadGeometry(reader, 9);
        var (x, y) = SpatialRowMapping.GetTile(geometry, SpatialRowMapping.AdminBoundaryZoom);

        return new StoredFeature
        {
            Id = id,
            FeatureId = SpatialRowMapping.GetText(properties, "featureId")
                ?? StoredFeature.NormalizeFeatureId(FeatureKinds.AdminBoundary, id),
            Kind = FeatureKinds.AdminBoundary,
            X = x,
            Y = y,
            Zoom = SpatialRowMapping.AdminBoundaryZoom,
            Geometry = geometry,
            Properties = properties,
        };
    }

    private static void Overlay(IDictionary<string, dynamic> properties, string key, string? value)
    {
        if (value is not null && !properties.ContainsKey(key))
            properties[key] = value;
    }
}
