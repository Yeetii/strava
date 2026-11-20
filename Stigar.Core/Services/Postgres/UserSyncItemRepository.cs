using Npgsql;
using Stigar.Core.Models;

namespace Stigar.Core.Services.Postgres;

/// <summary>
/// Reads and writes the <c>user_sync_items</c> table. <c>value</c> is JSONB and is exposed as the
/// model's raw JSON string; <c>DocumentType</c> is a Cosmos-only constant and is not persisted.
/// </summary>
public sealed class UserSyncItemRepository(NpgsqlDataSource dataSource) : IUserSyncItemRepository
{
    private const string SelectSql = """
        SELECT id, user_id, category, key, updated_at, deleted, value
        FROM user_sync_items
        """;

    private const string UpsertSql = """
        INSERT INTO user_sync_items (id, user_id, category, key, updated_at, deleted, value)
        VALUES (@id, @user_id, @category, @key, @updated_at, @deleted, @value)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            category = EXCLUDED.category,
            key = EXCLUDED.key,
            updated_at = EXCLUDED.updated_at,
            deleted = EXCLUDED.deleted,
            value = EXCLUDED.value
        """;

    public async Task<UserSyncItem?> GetById(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE id = @id LIMIT 1");
        command.Parameters.AddValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? Map(reader) : null;
    }

    public async Task<IReadOnlyList<UserSyncItem>> GetAllByUser(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id ORDER BY category, key");
        command.Parameters.AddValue("user_id", userId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var items = new List<UserSyncItem>();
        while (await reader.ReadAsync(cancellationToken))
            items.Add(Map(reader));
        return items;
    }

    public async Task<IReadOnlyList<UserSyncItem>> GetByUserAndCategory(
        string userId,
        string category,
        CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(SelectSql + " WHERE user_id = @user_id AND category = @category ORDER BY key");
        command.Parameters.AddValue("user_id", userId);
        command.Parameters.AddValue("category", category);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var items = new List<UserSyncItem>();
        while (await reader.ReadAsync(cancellationToken))
            items.Add(Map(reader));
        return items;
    }

    public async Task<IReadOnlyDictionary<string, long>> GetLatestUpdatedAtByUsers(CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT user_id, MAX(updated_at) FROM user_sync_items GROUP BY user_id");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var latest = new Dictionary<string, long>(StringComparer.Ordinal);
        while (await reader.ReadAsync(cancellationToken))
            latest[reader.GetString(0)] = reader.GetInt64(1);
        return latest;
    }

    public async Task<long?> GetLatestUpdatedAt(string userId, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "SELECT MAX(updated_at) FROM user_sync_items WHERE user_id = @user_id");
        command.Parameters.AddValue("user_id", userId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken) || await reader.IsDBNullAsync(0, cancellationToken))
            return null;
        return reader.GetInt64(0);
    }

    public async Task Upsert(UserSyncItem item, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(item);
        await using var command = dataSource.CreateCommand(UpsertSql);
        AddParameters(command.Parameters, item);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task Delete(string id, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM user_sync_items WHERE id = @id");
        command.Parameters.AddValue("id", id);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task DeleteByKey(string userId, string category, string key, CancellationToken cancellationToken = default)
    {
        await using var command = dataSource.CreateCommand(
            "DELETE FROM user_sync_items WHERE user_id = @user_id AND category = @category AND key = @key");
        command.Parameters.AddValue("user_id", userId);
        command.Parameters.AddValue("category", category);
        command.Parameters.AddValue("key", key);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddParameters(NpgsqlParameterCollection parameters, UserSyncItem item)
    {
        parameters.AddValue("id", item.Id);
        parameters.AddValue("user_id", item.UserId);
        parameters.AddValue("category", item.Category);
        parameters.AddValue("key", item.Key);
        parameters.AddValue("updated_at", item.UpdatedAt);
        parameters.AddValue("deleted", item.Deleted);
        parameters.AddJsonb("value", string.IsNullOrWhiteSpace(item.ValueJson) ? null : item.ValueJson);
    }

    private static UserSyncItem Map(NpgsqlDataReader reader) => new()
    {
        Id = reader.GetString(0),
        DocumentType = UserSyncItem.DocumentTypeValue,
        UserId = reader.GetString(1),
        Category = reader.GetString(2),
        Key = reader.GetString(3),
        UpdatedAt = reader.GetInt64(4),
        Deleted = reader.GetBoolean(5),
        ValueJson = PostgresValueMapping.ReadJson(reader, 6),
    };
}
