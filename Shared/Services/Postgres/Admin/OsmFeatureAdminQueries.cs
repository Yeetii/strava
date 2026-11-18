using Npgsql;
using NpgsqlTypes;
using Shared.Geo;
using Shared.Models;
using Shared.Services.Postgres;

namespace Shared.Services.Postgres.Admin;

/// <summary>Admin-only queries over the PostGIS OSM domain tables.</summary>
public sealed class OsmFeatureAdminQueries(NpgsqlDataSource dataSource) : IOsmFeatureAdminQueries
{
    private const string CountryCodeExpression =
        "COALESCE(properties ->> 'countryCode', properties ->> 'ISO3166-1', " +
        "properties ->> 'ISO3166-1:alpha2', properties ->> 'country_code_iso3166_1_alpha_2')";

    public async Task<IReadOnlyList<AdminCountryEntry>> GetCountriesAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand($"""
            SELECT id, name, {CountryCodeExpression} AS country_code
            FROM admin_boundaries
            WHERE admin_level = 2
              AND name IS NOT NULL
              AND name <> ''
            ORDER BY id
            """);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<AdminCountryEntry>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new AdminCountryEntry(
                reader.GetString(0),
                PostgresValueMapping.ReadText(reader, 1),
                PostgresValueMapping.ReadText(reader, 2)));
        }

        return result;
    }

    public async Task<IReadOnlyList<string>> GetCorruptedAdminBoundaryIdsAsync(CancellationToken cancellationToken = default)
    {
        // The Postgres `name` column is TEXT NOT NULL, so the leftover corruption shape is a
        // non-string JSON value under properties.name (the old Cosmos doc had an object there).
        await using var command = dataSource.CreateCommand("""
            SELECT id
            FROM admin_boundaries
            WHERE jsonb_typeof(properties -> 'name') IS NOT NULL
              AND jsonb_typeof(properties -> 'name') <> 'string'
            ORDER BY id
            """);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(reader.GetString(0));
        return result;
    }

    public async Task<IReadOnlyList<string>> GetAllAdminBoundaryIdsAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("SELECT id FROM admin_boundaries ORDER BY id");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<string>();
        while (await reader.ReadAsync(cancellationToken))
            result.Add(reader.GetString(0));
        return result;
    }

    public async Task<int> DeleteOsmTileQueriesAsync(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            DELETE FROM osm_tile_queries WHERE kind = @kind
            """);
        command.Parameters.AddValue("kind", FeatureKinds.AdminBoundary);
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<int> DeleteOsmFeaturesAsync(
        OsmFeatureTable table,
        int? zoom,
        IReadOnlyCollection<string>? ids,
        CancellationToken cancellationToken = default)
    {
        if (!ZoomMatches(table, zoom))
            return 0;

        var tableSql = TableName(table);
        await using var command = dataSource.CreateCommand();
        var conditions = new List<string>();
        if (ids is { Count: > 0 })
        {
            conditions.Add("id = ANY(@ids)");
            command.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
            {
                Value = ids.Where(id => !string.IsNullOrWhiteSpace(id))
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
            });
        }

        command.CommandText = conditions.Count == 0
            ? $"DELETE FROM {tableSql}"
            : $"DELETE FROM {tableSql} WHERE {string.Join(" AND ", conditions)}";

        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<OsmRetileResult> RetileOsmFeaturesAsync(
        OsmFeatureTable table,
        int? fromZoom,
        int toZoom,
        IReadOnlyCollection<string>? ids,
        CancellationToken cancellationToken = default)
    {
        if (!ZoomMatches(table, fromZoom))
            return new OsmRetileResult(0, 0);

        var tableSql = TableName(table);
        var select = $"SELECT id, ST_X(ST_Centroid(geom)) AS cx, ST_Y(ST_Centroid(geom)) AS cy FROM {tableSql}";
        var rows = new List<OsmRetileRow>();

        await using (var readCommand = dataSource.CreateCommand())
        {
            var conditions = new List<string>();
            if (ids is { Count: > 0 })
            {
                conditions.Add("id = ANY(@ids)");
                readCommand.Parameters.Add(new NpgsqlParameter("ids", NpgsqlDbType.Array | NpgsqlDbType.Text)
                {
                    Value = ids.Where(id => !string.IsNullOrWhiteSpace(id))
                        .Distinct(StringComparer.Ordinal)
                        .ToArray(),
                });
            }

            readCommand.CommandText = conditions.Count == 0
                ? select + " ORDER BY id"
                : select + " WHERE " + string.Join(" AND ", conditions) + " ORDER BY id";

            await using var reader = await readCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(new OsmRetileRow(
                    reader.GetString(0),
                    reader.GetDouble(1),
                    reader.GetDouble(2)));
            }
        }

        if (rows.Count == 0)
            return new OsmRetileResult(0, 0);

        await using var batch = dataSource.CreateBatch();
        foreach (var row in rows)
        {
            var (x, y) = SlippyTileCalculator.WGS84ToTileIndex(
                new Coordinate(row.CentroidLng, row.CentroidLat),
                toZoom);

            var update = new NpgsqlBatchCommand($"UPDATE {tableSql} SET tile_x = @x, tile_y = @y WHERE id = @id");
            update.Parameters.AddValue("x", x);
            update.Parameters.AddValue("y", y);
            update.Parameters.AddValue("id", row.Id);
            batch.BatchCommands.Add(update);
        }

        await batch.ExecuteNonQueryAsync(cancellationToken);
        return new OsmRetileResult(rows.Count, rows.Count);
    }

    public async Task<IReadOnlyList<AdminBoundaryArea>> ResolveAdminBoundaryAreasAsync(
        IReadOnlyList<string>? countryCodes,
        IReadOnlyList<(string osmType, long osmId)>? osmIds,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand();
        var sql = """
            SELECT id, ST_XMin(geom) AS min_lng, ST_YMin(geom) AS min_lat,
                   ST_XMax(geom) AS max_lng, ST_YMax(geom) AS max_lat
            FROM admin_boundaries
            WHERE admin_level = 2
            """;

        if (countryCodes is { Count: > 0 })
        {
            sql += $" AND lower({CountryCodeExpression}) = ANY(@codes)";
            command.Parameters.Add(new NpgsqlParameter("codes", NpgsqlDbType.Array | NpgsqlDbType.Text)
            {
                Value = countryCodes
                    .Where(code => !string.IsNullOrWhiteSpace(code))
                    .Select(code => code.ToLowerInvariant())
                    .Distinct(StringComparer.Ordinal)
                    .ToArray(),
            });
        }

        if (osmIds is { Count: > 0 })
        {
            var pairs = new List<string>();
            var index = 0;
            foreach (var (osmType, osmId) in osmIds.Distinct())
            {
                pairs.Add($"(osm_type = @osm_type_{index} AND osm_id = @osm_id_{index})");
                command.Parameters.AddValue($"osm_type_{index}", osmType);
                command.Parameters.AddValue($"osm_id_{index}", osmId);
                index++;
            }

            if (pairs.Count > 0)
                sql += " AND (" + string.Join(" OR ", pairs) + ")";
        }

        command.CommandText = sql + " ORDER BY id";

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var result = new List<AdminBoundaryArea>();
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new AdminBoundaryArea(
                reader.GetString(0),
                reader.GetDouble(1),
                reader.GetDouble(2),
                reader.GetDouble(3),
                reader.GetDouble(4)));
        }

        return result;
    }

    private static string TableName(OsmFeatureTable table) => table switch
    {
        OsmFeatureTable.Peaks => "peaks",
        OsmFeatureTable.ProtectedAreas => "protected_areas",
        OsmFeatureTable.AdminBoundaries => "admin_boundaries",
        _ => throw new ArgumentOutOfRangeException(nameof(table), table, "Unsupported OSM feature table."),
    };

    private static int StorageZoom(OsmFeatureTable table) => table switch
    {
        OsmFeatureTable.Peaks => AdminOsmConstants.PeakStorageZoom,
        OsmFeatureTable.ProtectedAreas => AdminOsmConstants.ProtectedAreaStorageZoom,
        OsmFeatureTable.AdminBoundaries => AdminOsmConstants.AdminBoundaryStorageZoom,
        _ => throw new ArgumentOutOfRangeException(nameof(table), table, "Unsupported OSM feature table."),
    };

    private static bool ZoomMatches(OsmFeatureTable table, int? zoom)
        => !zoom.HasValue || zoom.Value == StorageZoom(table);
}
