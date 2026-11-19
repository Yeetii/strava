using Npgsql;
using Stigar.Core.Services.Postgres;

namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>Reads and writes the <c>osm_tile_queries</c> marker table (idempotent writes).</summary>
public sealed class OsmTileQueryRepository(NpgsqlDataSource dataSource) : IOsmTileQueryRepository
{
    public async Task<bool> HasBeenQueried(
        string kind,
        int zoom,
        int x,
        int y,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            SELECT EXISTS (
                SELECT 1 FROM osm_tile_queries
                WHERE kind = @kind AND zoom = @zoom AND x = @x AND y = @y)
            """);
        AddKey(command.Parameters, kind, zoom, x, y);
        return await command.ExecuteScalarAsync(cancellationToken) is bool exists && exists;
    }

    public async Task MarkQueried(
        string kind,
        int zoom,
        int x,
        int y,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            INSERT INTO osm_tile_queries (kind, zoom, x, y)
            VALUES (@kind, @zoom, @x, @y)
            ON CONFLICT (kind, zoom, x, y) DO NOTHING
            """);
        AddKey(command.Parameters, kind, zoom, x, y);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(
        string kind,
        int zoom,
        int x,
        int y,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("""
            DELETE FROM osm_tile_queries
            WHERE kind = @kind AND zoom = @zoom AND x = @x AND y = @y
            """);
        AddKey(command.Parameters, kind, zoom, x, y);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddKey(NpgsqlParameterCollection parameters, string kind, int zoom, int x, int y)
    {
        parameters.AddValue("kind", kind);
        parameters.AddValue("zoom", zoom);
        parameters.AddValue("x", x);
        parameters.AddValue("y", y);
    }
}
