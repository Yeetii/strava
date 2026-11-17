namespace Shared.Services.Shards;

/// <summary>
/// Byte-oriented persistence for low-zoom highway shard indexes. Paths are relative
/// and use '/' separators so the same layout works on Blob Storage and on local disk.
/// A missing path is always represented by <c>null</c>, never an exception.
/// </summary>
public interface IShardIndexStore
{
    Task<byte[]?> TryReadAsync(string path, CancellationToken cancellationToken = default);
    Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default);
    Task<DateTimeOffset?> TryGetLastModifiedAsync(string path, CancellationToken cancellationToken = default);
}
