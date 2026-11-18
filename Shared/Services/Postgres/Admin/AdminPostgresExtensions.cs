using Microsoft.Extensions.DependencyInjection;

namespace Shared.Services.Postgres.Admin;

/// <summary>
/// Registers the admin-specific PostgreSQL query layer. The caller must have already registered
/// the <see cref="Npgsql.NpgsqlDataSource"/> singleton.
/// </summary>
public static class AdminPostgresServiceCollectionExtensions
{
    public static IServiceCollection AddPostgresAdminQueries(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IAdminStatusRepository, AdminStatusRepository>();
        services.AddSingleton<IOsmFeatureAdminQueries, OsmFeatureAdminQueries>();
        return services;
    }
}
