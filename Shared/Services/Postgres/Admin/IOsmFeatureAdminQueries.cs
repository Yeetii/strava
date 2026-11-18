namespace Shared.Services.Postgres.Admin;

/// <summary>Domain table behind an OSM feature kind. There is no table for paths or races.</summary>
public enum OsmFeatureTable
{
    Peaks,
    ProtectedAreas,
    AdminBoundaries,
}

/// <summary>Storage zooms used by <c>OsmFeatureIngestionService</c> (no zoom columns on the tables).</summary>
public static class AdminOsmConstants
{
    public const int AdminBoundaryStorageZoom = 6;
    public const int PeakStorageZoom = 11;
    public const int ProtectedAreaStorageZoom = 8;
}

public record AdminCountryEntry(string Id, string? Name, string? CountryCode);

public record AdminBoundaryArea(string Id, double MinLng, double MinLat, double MaxLng, double MaxLat);

public record OsmRetileRow(string Id, double CentroidLng, double CentroidLat);

public record OsmRetileResult(int Scanned, int Moved);

/// <summary>
/// Read/admin maintenance queries over the PostGIS domain tables. Kept separate from the normal
/// spatial repositories so admin-only operations do not widen their interfaces.
/// </summary>
public interface IOsmFeatureAdminQueries
{
    Task<IReadOnlyList<AdminCountryEntry>> GetCountriesAsync(CancellationToken cancellationToken = default);

    Task<IReadOnlyList<string>> GetCorruptedAdminBoundaryIdsAsync(CancellationToken cancellationToken = default);

    Task<IReadOnlyList<string>> GetAllAdminBoundaryIdsAsync(CancellationToken cancellationToken = default);

    /// <summary>Removes the OSM tile-query markers that replaced <c>empty-*</c> documents.</summary>
    Task<int> DeleteOsmTileQueriesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes rows from the given domain table. <paramref name="zoom"/> is a storage-zoom guard:
    /// the domain tables have no zoom column, so a zoom that differs from the table's fixed
    /// storage zoom matches no rows.
    /// </summary>
    Task<int> DeleteOsmFeaturesAsync(
        OsmFeatureTable table,
        int? zoom,
        IReadOnlyCollection<string>? ids,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Recomputes tile_x/tile_y at <paramref name="toZoom"/> from each geometry's centroid.
    /// Returns the number of rows scanned and updated.
    /// </summary>
    Task<OsmRetileResult> RetileOsmFeaturesAsync(
        OsmFeatureTable table,
        int? fromZoom,
        int toZoom,
        IReadOnlyCollection<string>? ids,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves supplied ISO codes / OSM ids to the bounding boxes of already-stored level-2
    /// boundaries. With no filters, returns every stored level-2 boundary area.
    /// </summary>
    Task<IReadOnlyList<AdminBoundaryArea>> ResolveAdminBoundaryAreasAsync(
        IReadOnlyList<string>? countryCodes,
        IReadOnlyList<(string osmType, long osmId)>? osmIds,
        CancellationToken cancellationToken = default);
}
