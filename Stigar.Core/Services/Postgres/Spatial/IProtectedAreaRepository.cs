using Stigar.Core.Models;

namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>Reads and writes the <c>protected_areas</c> domain table.</summary>
public interface IProtectedAreaRepository
{
    Task<StoredFeature?> GetById(string id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<StoredFeature>> GetByIds(IEnumerable<string> ids, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<StoredFeature>> GetByTile(int x, int y, CancellationToken cancellationToken = default);
    Task Upsert(StoredFeature area, CancellationToken cancellationToken = default);
    Task BulkUpsert(IEnumerable<StoredFeature> areas, CancellationToken cancellationToken = default);

    /// <summary>Areas whose geometry intersects the supplied point (ST_Intersects).</summary>
    Task<IReadOnlyList<StoredFeature>> FindContainingPoint(Coordinate point, CancellationToken cancellationToken = default);

    /// <summary>Lightweight summaries of areas containing any of the supplied points.</summary>
    Task<IReadOnlyList<StoredFeatureSummary>> FindSummariesContainingAnyPoint(
        IEnumerable<Coordinate> points,
        CancellationToken cancellationToken = default);

    Task Delete(string id, CancellationToken cancellationToken = default);
}
