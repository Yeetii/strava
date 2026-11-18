using Microsoft.Extensions.DependencyInjection;

namespace Shared.Services.Postgres.Admin;

/// <summary>
/// Registers the Backend-only PostgreSQL query layer used by the worker/backfill cutover. An
/// <see cref="Npgsql.NpgsqlDataSource"/> singleton must already be registered by the host.
/// </summary>
public static class BackendPostgresExtensions
{
    /// <summary>
    /// Adds <see cref="IBackendAdminQueries"/>. Call from both the API and Backend hosts after
    /// the shared core/spatial repository registrations.
    /// </summary>
    public static IServiceCollection AddBackendAdminQueries(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IBackendAdminQueries, BackendAdminQueries>();
        return services;
    }
}
