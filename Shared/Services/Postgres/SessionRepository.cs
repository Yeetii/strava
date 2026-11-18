using Npgsql;
using Shared.Models;

namespace Shared.Services.Postgres;

/// <summary>
/// Reads and writes the <c>sessions</c> table. <c>created_at_utc</c> has a DDL default, but the
/// model value is written explicitly so migrated sessions keep their original creation time.
/// </summary>
public sealed class SessionRepository(NpgsqlDataSource dataSource) : ISessionRepository
{
    private const string SelectSql = """
        SELECT id, user_id, created_at_utc
        FROM sessions
        """;

    private const string UpsertSql = """
        INSERT INTO sessions (id, user_id, created_at_utc)
        VALUES (@id, @user_id, @created_at_utc)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            created_at_utc = EXCLUDED.created_at_utc
        """;

    public async Task<Session?> GetById(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE id = @id LIMIT 1");
        command.Parameters.AddValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? Map(reader) : null;
    }

    public async Task<IReadOnlyList<Session>> GetByUserId(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id ORDER BY created_at_utc DESC, id");
        command.Parameters.AddValue("user_id", userId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var sessions = new List<Session>();
        while (await reader.ReadAsync(cancellationToken))
            sessions.Add(Map(reader));
        return sessions;
    }

    public async Task Upsert(Session session, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        await using var command = dataSource.CreateCommand(UpsertSql);
        AddParameters(command.Parameters, session);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM sessions WHERE id = @id");
        command.Parameters.AddValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DeleteByUserId(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM sessions WHERE user_id = @user_id");
        command.Parameters.AddValue("user_id", userId);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, Session session)
    {
        parameters.AddValue("id", session.Id);
        parameters.AddValue("user_id", session.UserId);
        parameters.AddValue("created_at_utc", PostgresValueMapping.AsUtc(session.CreatedAtUtc));
    }

    private static Session Map(NpgsqlDataReader reader) => new()
    {
        Id = reader.GetString(0),
        UserId = reader.GetString(1),
        CreatedAtUtc = reader.GetDateTime(2),
    };
}
