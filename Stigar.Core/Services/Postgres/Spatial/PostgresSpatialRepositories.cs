using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Stigar.Core.Services.Postgres.Spatial;

/// <summary>
/// Registers the PostGIS spatial repositories. An <see cref="NpgsqlDataSource"/> singleton must
/// already be registered by the caller (T5). Repositories are singletons with no per-request
/// state, mirroring <c>PostgresCoreRepositoryServiceCollectionExtensions</c>.
/// </summary>
public static class PostgresSpatialRepositoryServiceCollectionExtensions
{
    public static IServiceCollection AddPostgresSpatialRepositories(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IPeakRepository, PeakRepository>();
        services.AddSingleton<IProtectedAreaRepository, ProtectedAreaRepository>();
        services.AddSingleton<IAdminBoundaryRepository, AdminBoundaryRepository>();
        services.AddSingleton<IOsmTileQueryRepository, OsmTileQueryRepository>();
        services.AddSingleton<IActivitySummitRepository, ActivitySummitRepository>();
        services.AddSingleton<IActivityVisitedPathRepository, ActivityVisitedPathRepository>();
        services.AddSingleton<IActivityVisitedAreaRepository, ActivityVisitedAreaRepository>();
        return services;
    }
}
