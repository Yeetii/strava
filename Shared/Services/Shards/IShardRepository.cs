using Shared.Models;

namespace Shared.Services.Shards;

public interface IShardRepository
{
    Task<Shard> GetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default);
    Task<Shard?> TryGetShardAsync(int z, int x, int y, CancellationToken cancellationToken = default);
    Task<DateTimeOffset?> TryGetShardLastModifiedAsync(int z, int x, int y, CancellationToken cancellationToken = default);
    Task DeleteShardAsync(int z, int x, int y, CancellationToken cancellationToken = default);
}
