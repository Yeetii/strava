using Shared.Models;

namespace Shared.Services.Postgres;

public interface IActivityRepository
{
    Task<Activity?> GetById(string id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<Activity>> GetByIds(IEnumerable<string> ids, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<Activity>> GetAllByUser(string userId, CancellationToken cancellationToken = default);
    Task<IReadOnlyDictionary<string, int>> GetActivityCountsByUser(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> GetAllIds(CancellationToken cancellationToken = default);
    Task Upsert(Activity activity, CancellationToken cancellationToken = default);
    Task BulkUpsert(IEnumerable<Activity> activities, CancellationToken cancellationToken = default);
    Task Delete(string id, CancellationToken cancellationToken = default);
    Task UpdateProcessingStatus(string id, ActivityProcessingStatus? status, CancellationToken cancellationToken = default);
}
