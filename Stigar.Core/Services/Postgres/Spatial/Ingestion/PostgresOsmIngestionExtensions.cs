using Microsoft.Extensions.DependencyInjection;

namespace Stigar.Core.Services.Postgres.Spatial.Ingestion;

/// <summary>
/// Registers the on-demand Overpass-to-PostGIS ingestion service. The caller must have already
/// registered <c>OverpassClient</c>, <c>IOsmTileQueryRepository</c> and the three spatial domain
/// repositories (via <c>AddPostgresSpatialRepositories</c>).
/// </summary>
public static class PostgresOsmIngestionExtensions
{
    public static IServiceCollection AddPostgresOsmIngestion(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IOsmFeatureIngestionService, OsmFeatureIngestionService>();
        return services;
    }
}
