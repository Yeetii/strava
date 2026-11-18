using Shared.Models;

namespace Shared.Services.Postgres.Spatial.Ingestion;

/// <summary>
/// Restores on-demand Overpass ingestion for the PostGIS-backed workers. When a storage tile
/// covering a route has not been queried yet, the matching OSM features are fetched from
/// Overpass and upserted into the PostGIS domain tables before the spatial lookup runs.
/// </summary>
public interface IOsmFeatureIngestionService
{
    Task<OsmIngestionResult> EnsurePeaksAsync(
        IEnumerable<Coordinate> routePoints,
        CancellationToken cancellationToken = default);

    Task<OsmIngestionResult> EnsureProtectedAreasAsync(
        IEnumerable<Coordinate> routePoints,
        CancellationToken cancellationToken = default);

    Task<OsmIngestionResult> EnsureAdminBoundariesAsync(
        IEnumerable<Coordinate> routePoints,
        int adminLevel,
        CancellationToken cancellationToken = default);
}

/// <summary>Counts observed while ensuring that the OSM features covering a route are stored.</summary>
public sealed record OsmIngestionResult(
    int TilesQueried,
    int FeaturesFetched,
    int FeaturesUpserted,
    int TilesFailed)
{
    public static OsmIngestionResult Empty { get; } = new(0, 0, 0, 0);

    public bool AnyWorkDone => TilesQueried > 0 || TilesFailed > 0;
}
