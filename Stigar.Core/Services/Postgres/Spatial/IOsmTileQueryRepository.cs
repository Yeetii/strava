namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>
/// Tracks which OSM tile queries have already been issued. This replaces the Cosmos
/// <c>empty-*</c> marker documents that previously suppressed repeat fetches.
/// </summary>
public interface IOsmTileQueryRepository
{
    Task<bool> HasBeenQueried(string kind, int zoom, int x, int y, CancellationToken cancellationToken = default);
    Task MarkQueried(string kind, int zoom, int x, int y, CancellationToken cancellationToken = default);
    Task Delete(string kind, int zoom, int x, int y, CancellationToken cancellationToken = default);
}
