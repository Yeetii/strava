using Stigar.Core.Models;

namespace Stigar.Core.Services.Postgres;

public interface IUserRepository
{
    Task<User?> GetById(string id, CancellationToken cancellationToken = default);
    Task<User?> GetByUserName(string userName, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<User>> GetAll(CancellationToken cancellationToken = default);
    Task Upsert(User user, CancellationToken cancellationToken = default);
    Task Delete(string id, CancellationToken cancellationToken = default);
}
