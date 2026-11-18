using Shared.Models;

namespace Shared.Services.Postgres;

public interface ISessionRepository
{
    Task<Session?> GetById(string id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<Session>> GetByUserId(string userId, CancellationToken cancellationToken = default);
    Task<IReadOnlyDictionary<string, DateTime>> GetLastLoginByUsers(CancellationToken cancellationToken = default);
    Task Upsert(Session session, CancellationToken cancellationToken = default);
    Task Delete(string id, CancellationToken cancellationToken = default);
    Task DeleteByUserId(string userId, CancellationToken cancellationToken = default);
}
