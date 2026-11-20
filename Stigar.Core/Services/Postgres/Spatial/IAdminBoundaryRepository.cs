using BAMCIS.GeoJSON;
using Stigar.Core.Models;

namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>Reads and writes the <c>admin_boundaries</c> domain table.</summary>
public interface IAdminBoundaryRepository
{
    Task<StoredFeature?> GetById(string id, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<StoredFeature>> GetByIds(IEnumerable<string> ids, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<StoredFeature>> GetByLevel(int adminLevel, CancellationToken cancellationToken = default);

    /// <summary>Boundaries intersecting the WGS84 envelope of the given slippy tile.</summary>
    Task<IReadOnlyList<StoredFeature>> GetByTile(int x, int y, int adminLevel, int zoom, CancellationToken cancellationToken = default);

    Task Upsert(StoredFeature boundary, CancellationToken cancellationToken = default);
    Task BulkUpsert(IEnumerable<StoredFeature> boundaries, CancellationToken cancellationToken = default);

    /// <summary>Sets the optional simplified geometry column.</summary>
    Task SetSimplifiedGeometry(string id, Geometry simplified, CancellationToken cancellationToken = default);

    /// <summary>Merges metric keys into the JSONB properties and stamps the completed epoch.</summary>
    Task UpdateMetrics(
        string id,
        IReadOnlyDictionary<string, object?> metrics,
        long epoch,
        CancellationToken cancellationToken = default);

    /// <summary>Lightweight summaries of boundaries containing any of the supplied points.</summary>
    Task<IReadOnlyList<StoredFeatureSummary>> FindSummariesContainingAnyPoint(
        IEnumerable<Coordinate> points,
        int? adminLevel = null,
        CancellationToken cancellationToken = default);

    /// <summary>Deletes every boundary row; returns the number of rows removed.</summary>
    Task<int> DeleteAll(CancellationToken cancellationToken = default);

    Task Delete(string id, CancellationToken cancellationToken = default);
}
