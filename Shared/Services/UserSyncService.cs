using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Shared.Models;

namespace Shared.Services;

public sealed record UserSyncPayload(IReadOnlyList<UserSyncEntry> Settings, IReadOnlyList<UserSyncEntry> Files);

public sealed record UserSyncEntry(string Key, long UpdatedAt, bool Deleted, JsonElement? Value);

public class UserSyncService(CollectionClient<UserSyncItem> syncCollection)
{
    public async Task<UserSyncPayload> GetSnapshot(string userId, CancellationToken cancellationToken = default)
    {
        var itemsById = await GetItemsByUserId(userId, cancellationToken);
        return ToPayload(itemsById.Values);
    }

    public async Task<UserSyncPayload> ApplyChanges(string userId, UserSyncPayload payload, CancellationToken cancellationToken = default)
    {
        var itemsById = await GetItemsByUserId(userId, cancellationToken);

        await ApplyCategoryChanges(userId, UserSyncItem.SettingsCategory, payload.Settings, itemsById, cancellationToken);
        await ApplyCategoryChanges(userId, UserSyncItem.FilesCategory, payload.Files, itemsById, cancellationToken);

        return ToPayload(itemsById.Values);
    }

    private async Task<Dictionary<string, UserSyncItem>> GetItemsByUserId(string userId, CancellationToken cancellationToken)
    {
        var query = new QueryDefinition(
            "SELECT * FROM c WHERE c.documentType = @documentType AND c.userId = @userId")
            .WithParameter("@documentType", UserSyncItem.DocumentTypeValue)
            .WithParameter("@userId", userId);

        var items = await syncCollection.ExecuteQueryAsync<UserSyncItem>(query, cancellationToken: cancellationToken);
        return items.ToDictionary(item => item.Id, item => item);
    }

    private async Task ApplyCategoryChanges(
        string userId,
        string category,
        IReadOnlyList<UserSyncEntry> entries,
        Dictionary<string, UserSyncItem> itemsById,
        CancellationToken cancellationToken)
    {
        foreach (var entry in entries
            .Where(entry => !string.IsNullOrWhiteSpace(entry.Key))
            .GroupBy(entry => entry.Key)
            .Select(group => group.OrderByDescending(entry => entry.UpdatedAt).First()))
        {
            var itemId = CreateItemId(userId, category, entry.Key);
            itemsById.TryGetValue(itemId, out var currentItem);

            if (currentItem != null && entry.UpdatedAt < currentItem.UpdatedAt)
            {
                continue;
            }

            var updatedItem = new UserSyncItem
            {
                Id = itemId,
                DocumentType = UserSyncItem.DocumentTypeValue,
                UserId = userId,
                Category = category,
                Key = entry.Key,
                UpdatedAt = entry.UpdatedAt,
                Deleted = entry.Deleted,
                Value = entry.Deleted ? null : entry.Value
            };

            await syncCollection.UpsertDocument(updatedItem, cancellationToken);
            itemsById[itemId] = updatedItem;
        }
    }

    private static UserSyncPayload ToPayload(IEnumerable<UserSyncItem> items)
    {
        var settings = items
            .Where(item => item.Category == UserSyncItem.SettingsCategory)
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .Select(item => new UserSyncEntry(item.Key, item.UpdatedAt, item.Deleted, item.Value))
            .ToArray();

        var files = items
            .Where(item => item.Category == UserSyncItem.FilesCategory)
            .OrderBy(item => item.Key, StringComparer.Ordinal)
            .Select(item => new UserSyncEntry(item.Key, item.UpdatedAt, item.Deleted, item.Value))
            .ToArray();

        return new UserSyncPayload(settings, files);
    }

    public static string CreateItemId(string userId, string category, string key)
    {
        return $"userSync:{category}:{userId}:{key}";
    }
}