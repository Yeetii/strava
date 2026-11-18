using Shared.Models;

namespace Shared.Services.Postgres;

/// <summary>
/// Read/write access to the curated <c>peaks_groups</c> table, the PostgreSQL
/// replacement for the Cosmos <c>peaksGroups</c> container.
/// </summary>
public interface IPeaksGroupRepository
{
    Task<IReadOnlyList<PeaksGroup>> GetAll(CancellationToken cancellationToken = default);
    Task<PeaksGroup?> GetById(Guid id, CancellationToken cancellationToken = default);
    Task Upsert(PeaksGroup group, CancellationToken cancellationToken = default);
}
