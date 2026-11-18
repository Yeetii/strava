using System.Globalization;
using Npgsql;
using NpgsqlTypes;
using Shared.Models;
using Shared.Services.Postgres;

namespace Shared.Services.Postgres.Spatial;

/// <summary>
/// Reads and writes the <c>peaks</c> table. Geometry crosses the boundary as GeoJSON text via
/// <c>ST_GeomFromGeoJSON</c>/<c>ST_AsGeoJSON</c>. Spatial predicates use the GIST index
/// <c>idx_peaks_geom</c> (via a bounding-box prefilter and ST_Intersects) and the b-tree index
/// <c>idx_peaks_tile</c> for tile lookups.
/// </summary>
public sealed class PeakRepository(NpgsqlDataSource dataSource) : IPeakRepository
{
    private const string SelectSql = """
        SELECT id, name, elevation, tile_x, tile_y, ST_AsGeoJSON(geom) AS geom_json
        FROM peaks
        """;

    private const string UpsertSql = """
        INSERT INTO peaks (id, name, elevation, tile_x, tile_y, geom)
        VALUES (@id, @name, @elevation, @tile_x, @tile_y, ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326))
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            elevation = EXCLUDED.elevation,
            tile_x = EXCLUDED.tile_x,
            tile_y = EXCLUDED.tile_y,
            geom = EXCLUDED.geom
        """;

    private const string RadiusPredicate =
        " WHERE geom && ST_Expand(ST_SetSRID(ST_MakePoint(@lng, @lat), 4326), @radius_degrees)" +
        " AND ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(@lng, @lat), 4326)::geography, @radius)";

    private const string ContainingPredicate =
        " WHERE ST_Intersects(geom, ST_SetSRID(ST_MakePoint(@lng, @lat), 4326))";

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

    public async Task<IReadOnlyList<StoredFeature>> FindWithinRadius(
        Coordinate center,
        int radiusMeters,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(center);

        // The bbox prefilter lets PostgreSQL use the geometry GIST index; the geography cast
        // keeps the distance test in metres as the Cosmos ST_DISTANCE implementation did.
        var latitudeRadians = center.Lat * Math.PI / 180.0;
        var radiusDegrees = radiusMeters / (111_320.0 * Math.Max(Math.Cos(latitudeRadians), 0.01));

        await using var command = dataSource.CreateCommand(SelectSql + RadiusPredicate + " ORDER BY id");
        AddPoint(command.Parameters, center);
        command.Parameters.AddValue("radius", radiusMeters);
        command.Parameters.AddValue("radius_degrees", radiusDegrees);
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
        AddPoint(command.Parameters, point);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<StoredFeature>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(Map(reader));
        return result;
    }

    public async Task Upsert(StoredFeature peak, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(peak);
        await using var command = dataSource.CreateCommand(UpsertSql);
        AddParameters(command.Parameters, peak);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task BulkUpsert(IEnumerable<StoredFeature> peaks, CancellationToken cancellationToken = default)
    {
        var list = (peaks ?? []).ToList();
        if (list.Count == 0)
            return;

        await using var batch = dataSource.CreateBatch();
        foreach (var peak in list)
        {
            ArgumentNullException.ThrowIfNull(peak);
            var command = new NpgsqlBatchCommand(UpsertSql);
            AddParameters(command.Parameters, peak);
            batch.BatchCommands.Add(command);
        }
        await batch.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM peaks WHERE id = @id");
        command.Parameters.AddValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddPoint(NpgsqlParameterCollection parameters, Coordinate point)
    {
        parameters.AddValue("lng", point.Lng);
        parameters.AddValue("lat", point.Lat);
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, StoredFeature peak)
    {
        var elevation = SpatialRowMapping.GetDouble(peak.Properties, "ele")
            ?? SpatialRowMapping.GetDouble(peak.Properties, "elevation");

        parameters.AddValue("id", peak.Id);
        parameters.AddValue("name", SpatialRowMapping.GetText(peak.Properties, "name") ?? string.Empty);
        parameters.AddValue("elevation", elevation);
        parameters.AddValue("tile_x", peak.X);
        parameters.AddValue("tile_y", peak.Y);
        SpatialRowMapping.AddGeometry(parameters, "geom", peak.Geometry);
    }

    private static StoredFeature Map(NpgsqlDataReader reader)
    {
        var id = reader.GetString(0);
        var properties = new Dictionary<string, dynamic>();
        var name = PostgresValueMapping.ReadText(reader, 1);
        var elevation = PostgresValueMapping.ReadFloat(reader, 2);
        if (name is not null)
            properties["name"] = name;
        if (elevation.HasValue)
            properties["ele"] = elevation.Value.ToString(CultureInfo.InvariantCulture);

        return new StoredFeature
        {
            Id = id,
            FeatureId = StoredFeature.NormalizeFeatureId(FeatureKinds.Peak, id),
            Kind = FeatureKinds.Peak,
            X = reader.GetInt32(3),
            Y = reader.GetInt32(4),
            Zoom = SpatialRowMapping.PeakZoom,
            Geometry = SpatialRowMapping.ReadGeometry(reader, 5),
            Properties = properties,
        };
    }
}
