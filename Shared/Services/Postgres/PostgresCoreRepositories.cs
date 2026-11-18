using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Shared.Services.Postgres;

/// <summary>
/// Registers the five core PostgreSQL repositories. An <see cref="NpgsqlDataSource"/> singleton
/// must already be registered by the caller (T5). Repositories themselves are registered as
/// singletons too: they hold only the thread-safe data source and no per-request state, matching
/// the existing <c>CollectionClientBuilder</c> registration style for Cosmos clients.
/// </summary>
public static class PostgresCoreRepositoryServiceCollectionExtensions
{
    public static IServiceCollection AddPostgresCoreRepositories(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IUserRepository, UserRepository>();
        services.AddSingleton<ISessionRepository, SessionRepository>();
        services.AddSingleton<IUserSyncItemRepository, UserSyncItemRepository>();
        services.AddSingleton<IActivityRepository, ActivityRepository>();
        services.AddSingleton<IPeaksGroupRepository, PeaksGroupRepository>();
        return services;
    }
}
