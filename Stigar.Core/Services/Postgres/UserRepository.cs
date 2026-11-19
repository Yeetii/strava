using Npgsql;
using Stigar.Core.Models;

namespace Stigar.Core.Services.Postgres;

/// <summary>
/// Reads and writes the <c>users</c> table. The Cosmos-only <see cref="User.ETag"/> is not persisted.
/// </summary>
public sealed class UserRepository(NpgsqlDataSource dataSource) : IUserRepository
{
    private const string SelectSql = """
        SELECT id, user_name, first_name, last_name, refresh_token, access_token,
               token_expires_at, strava_scope, sync_status
        FROM users
        """;

    private const string UpsertSql = """
        INSERT INTO users (
            id, user_name, first_name, last_name, refresh_token, access_token,
            token_expires_at, strava_scope, sync_status
        ) VALUES (
            @id, @user_name, @first_name, @last_name, @refresh_token, @access_token,
            @token_expires_at, @strava_scope, @sync_status
        )
        ON CONFLICT (id) DO UPDATE SET
            user_name = EXCLUDED.user_name,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            refresh_token = EXCLUDED.refresh_token,
            access_token = EXCLUDED.access_token,
            token_expires_at = EXCLUDED.token_expires_at,
            strava_scope = EXCLUDED.strava_scope,
            sync_status = EXCLUDED.sync_status
        """;

    public async Task<User?> GetById(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE id = @id LIMIT 1");
        command.Parameters.AddValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? Map(reader) : null;
    }

    public async Task<User?> GetByUserName(string userName, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_name = @user_name ORDER BY id LIMIT 1");
        command.Parameters.AddValue("user_name", userName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? Map(reader) : null;
    }

    public async Task<IReadOnlyList<User>> GetAll(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " ORDER BY id");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var users = new List<User>();
        while (await reader.ReadAsync(cancellationToken))
            users.Add(Map(reader));
        return users;
    }

    public async Task Upsert(User user, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(user);
        await using var command = dataSource.CreateCommand(UpsertSql);
        AddParameters(command.Parameters, user);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM users WHERE id = @id");
        command.Parameters.AddValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, User user)
    {
        parameters.AddValue("id", user.Id);
        parameters.AddValue("user_name", user.UserName);
        parameters.AddValue("first_name", user.FirstName);
        parameters.AddValue("last_name", user.LastName);
        parameters.AddValue("refresh_token", user.RefreshToken);
        parameters.AddValue("access_token", user.AccessToken);
        parameters.AddValue("token_expires_at", user.TokenExpiresAt);
        parameters.AddValue("strava_scope", user.StravaScope);
        parameters.AddJsonb("sync_status", PostgresValueMapping.Serialize(user.SyncStatus));
    }

    private static User Map(NpgsqlDataReader reader) => new()
    {
        Id = reader.GetString(0),
        UserName = PostgresValueMapping.ReadText(reader, 1),
        FirstName = PostgresValueMapping.ReadText(reader, 2),
        LastName = PostgresValueMapping.ReadText(reader, 3),
        RefreshToken = PostgresValueMapping.ReadText(reader, 4),
        AccessToken = PostgresValueMapping.ReadText(reader, 5),
        TokenExpiresAt = reader.GetInt64(6),
        StravaScope = PostgresValueMapping.ReadText(reader, 7),
        SyncStatus = PostgresValueMapping.Deserialize<StravaSyncStatus>(PostgresValueMapping.ReadJson(reader, 8)),
    };
}
