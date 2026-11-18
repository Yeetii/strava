using Shared.Models;

namespace Shared.Services.Postgres;

public interface IUserSyncItemRepository
{
    Task<UserSyncItem?> GetById(string id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<UserSyncItem>> GetAllByUser(string userId, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<UserSyncItem>> GetByUserAndCategory(string userId, string category, CancellationToken cancellationToken = default);
    Task Upsert(UserSyncItem item, CancellationToken cancellationToken = default);
    Task Delete(string id, CancellationToken cancellationToken = default);
    Task DeleteByKey(string userId, string category, string key, CancellationToken cancellationToken = default);
}
