using Npgsql;
using NpgsqlTypes;
using Shared.Models;
using Shared.Services.Postgres;

namespace Shared.Services.Postgres.Spatial;

/// <summary>
/// Reads and writes the <c>protected_areas</c> table. Geometry crosses the boundary as GeoJSON
/// text; the <c>properties</c> JSONB column keeps the original OSM property bag. Containment
/// queries use <c>ST_Intersects</c>, which can use the GIST index <c>idx_protected_areas_geom</c>.
/// </summary>
public sealed class ProtectedAreaRepository(NpgsqlDataSource dataSource) : IProtectedAreaRepository
{
    private const string SelectSql = """
        SELECT id, name, area_type, wikidata, wikimedia_commons, tile_x, tile_y, properties,
               ST_AsGeoJSON(geom) AS geom_json
        FROM protected_areas
        """;

    private const string UpsertSql = """
        INSERT INTO protected_areas (
            id, name, area_type, wikidata, wikimedia_commons, tile_x, tile_y, properties, geom)
        VALUES (
            @id, @name, @area_type, @wikidata, @wikimedia_commons, @tile_x, @tile_y, @properties,
            ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326))
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            area_type = EXCLUDED.area_type,
            wikidata = EXCLUDED.wikidata,
            wikimedia_commons = EXCLUDED.wikimedia_commons,
            tile_x = EXCLUDED.tile_x,
            tile_y = EXCLUDED.tile_y,
            properties = EXCLUDED.properties,
            geom = EXCLUDED.geom
        """;

    private const string ContainingPredicate =
        " WHERE ST_Intersects(geom, ST_SetSRID(ST_MakePoint(@lng, @lat), 4326))";

    private const string ContainingAnySummarySql = """
        SELECT DISTINCT id, properties,
               ST_X(ST_Centroid(geom)) AS cx, ST_Y(ST_Centroid(geom)) AS cy
        FROM protected_areas
        WHERE EXISTS (
            SELECT 1 FROM unnest(@lngs::float8[], @lats::float8[]) AS q(lng, lat)
            WHERE ST_Intersects(geom, ST_SetSRID(ST_MakePoint(q.lng, q.lat), 4326)))
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

    public async Task<IReadOnlyList<StoredFeature>> GetByTile(int x, int y, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            SelectSql + " WHERE tile_x = @tile_x AND tile_y = @tile_y ORDER BY id");
        command.Parameters.AddValue("tile_x", x);
        command.Parameters.AddValue("tile_y", y);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<StoredFeature>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(Map(reader));
        return result;
    }

    public async Task<IReadOnlyList<StoredFeature>> FindContainingPoint(
        Coordinate point,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(point);

        await using var command = dataSource.CreateCommand(SelectSql + ContainingPredicate + " ORDER BY id");
        command.Parameters.AddValue("lng", point.Lng);
        command.Parameters.AddValue("lat", point.Lat);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<StoredFeature>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(Map(reader));
        return result;
    }

    public async Task<IReadOnlyList<StoredFeatureSummary>> FindSummariesContainingAnyPoint(
        IEnumerable<Coordinate> points,
        CancellationToken cancellationToken = default)
    {
        var distinct = (points ?? [])
            .Where(point => point is not null)
            .DistinctBy(point => (point.Lng, point.Lat))
            .ToArray();
        if (distinct.Length == 0)
            return [];

        await using var command = dataSource.CreateCommand(ContainingAnySummarySql);
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
                FeatureKinds.ProtectedArea,
                SpatialRowMapping.ReadProperties(reader, 1),
                new Coordinate(reader.GetDouble(2), reader.GetDouble(3)),
                SpatialRowMapping.ProtectedAreaZoom));
        }
        return result;
    }

    public async Task Upsert(StoredFeature area, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(area);
        await using var command = dataSource.CreateCommand(UpsertSql);
        AddParameters(command.Parameters, area);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task BulkUpsert(IEnumerable<StoredFeature> areas, CancellationToken cancellationToken = default)
    {
        var list = (areas ?? []).ToList();
        if (list.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var area in list)
        {
            ArgumentNullException.ThrowIfNull(area);
            var command = new NpgsqlBatchCommand(UpsertSql);
            AddParameters(command.Parameters, area);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM protected_areas WHERE id = @id");
        command.Parameters.AddValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, StoredFeature area)
    {
        var properties = area.Properties;
        parameters.AddValue("id", area.Id);
        parameters.AddValue("name", SpatialRowMapping.GetText(properties, "name") ?? string.Empty);
        parameters.AddValue("area_type", SpatialRowMapping.GetText(properties, "areaType")
            ?? SpatialRowMapping.GetText(properties, "area_type") ?? string.Empty);
        parameters.AddValue("wikidata", SpatialRowMapping.GetText(properties, "wikidata"));
        parameters.AddValue("wikimedia_commons", SpatialRowMapping.GetText(properties, "wikimediaCommons")
            ?? SpatialRowMapping.GetText(properties, "wikimedia_commons"));
        parameters.AddValue("tile_x", area.X);
        parameters.AddValue("tile_y", area.Y);
        parameters.AddJsonb("properties", SpatialRowMapping.PropertiesToJson(properties));
        SpatialRowMapping.AddGeometry(parameters, "geom", area.Geometry);
    }

    private static StoredFeature Map(NpgsqlDataReader reader)
    {
        var id = reader.GetString(0);
        var properties = SpatialRowMapping.ReadProperties(reader, 7);
        Overlay(properties, "name", PostgresValueMapping.ReadText(reader, 1));
        Overlay(properties, "areaType", PostgresValueMapping.ReadText(reader, 2));
        Overlay(properties, "wikidata", PostgresValueMapping.ReadText(reader, 3));
        Overlay(properties, "wikimediaCommons", PostgresValueMapping.ReadText(reader, 4));

        return new StoredFeature
        {
            Id = id,
            FeatureId = StoredFeature.NormalizeFeatureId(FeatureKinds.ProtectedArea, id),
            Kind = FeatureKinds.ProtectedArea,
            X = reader.GetInt32(5),
            Y = reader.GetInt32(6),
            Zoom = SpatialRowMapping.ProtectedAreaZoom,
            Geometry = SpatialRowMapping.ReadGeometry(reader, 8),
            Properties = properties,
        };
    }

    private static void Overlay(IDictionary<string, dynamic> properties, string key, string? value)
    {
        if (value is not null && !properties.ContainsKey(key))
            properties[key] = value;
    }
}
