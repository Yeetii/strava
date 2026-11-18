using Shared.Models;

namespace Shared.Services.Postgres.Spatial;

/// <summary>Reads and writes the <c>peaks</c> domain table.</summary>
public interface IPeakRepository
{
    Task<StoredFeature?> GetById(string id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<StoredFeature>> GetByIds(IEnumerable<string> ids, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<StoredFeature>> GetByTile(int x, int y, CancellationToken cancellationToken = default);
    Task Upsert(StoredFeature peak, CancellationToken cancellationToken = default);
    Task BulkUpsert(IEnumerable<StoredFeature> peaks, CancellationToken cancellationToken = default);

    /// <summary>Peaks within <paramref name="radiusMeters"/> of the centre (SRID 4326, metres).</summary>
    Task<IReadOnlyList<StoredFeature>> FindWithinRadius(Coordinate center, int radiusMeters, CancellationToken cancellationToken = default);

    /// <summary>Peaks whose point geometry intersects the supplied point.</summary>
    Task<IReadOnlyList<StoredFeature>> FindContainingPoint(Coordinate point, CancellationToken cancellationToken = default);

    Task Delete(string id, CancellationToken cancellationToken = default);
}
