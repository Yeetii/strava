namespace Shared.Services.Shards;

/// <summary>
/// Local-filesystem implementation of <see cref="IShardIndexStore"/>. Uses the same
/// relative <c>index/{z}/{x}/{y}.json</c> layout as the Blob container so an index
/// export can be copied verbatim into the shard root.
/// </summary>
public sealed class FileSystemShardIndexStore(string rootDir) : IShardIndexStore
{
    private readonly LocalShardFiles _files = new(rootDir);

    public Task<byte[]?> TryReadAsync(string path, CancellationToken cancellationToken = default)
    {
        var full = _files.Resolve(path);
        return Task.FromResult(File.Exists(full) ? File.ReadAllBytes(full) : null);
    }

    public Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default)
        => _files.WriteAtomicAsync(_files.Resolve(path), content, cancellationToken);

    public Task<DateTimeOffset?> TryGetLastModifiedAsync(string path, CancellationToken cancellationToken = default)
    {
        var full = _files.Resolve(path);
        return Task.FromResult<DateTimeOffset?>(
            File.Exists(full) ? new DateTimeOffset(File.GetLastWriteTimeUtc(full)) : null);
    }
}
