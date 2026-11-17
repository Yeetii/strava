namespace Shared.Services.Shards;

/// <summary>
/// Shared on-disk path resolution and atomic writes for the filesystem shard stores.
/// Both the shard and index stores stay inside their configured root, and readers
/// never see a half-written file because writes land via a temp file + atomic move.
/// </summary>
internal sealed class LocalShardFiles(string rootDir)
{
    private readonly string _root = Path.GetFullPath(rootDir);

    internal string Resolve(string relativePath)
    {
        var normalized = relativePath.Replace('/', Path.DirectorySeparatorChar);
        var path = Path.GetFullPath(Path.Combine(_root, normalized));
        if (!path.StartsWith(_root + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new InvalidOperationException($"'{relativePath}' escapes the configured shard root directory.");
        return path;
    }

    internal async Task WriteAtomicAsync(string path, byte[] content, CancellationToken cancellationToken)
    {
        var directory = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(directory);
        var temp = Path.Combine(directory, $".{Path.GetFileName(path)}.{Guid.NewGuid():N}.tmp");
        try
        {
            await File.WriteAllBytesAsync(temp, content, cancellationToken);
            File.Move(temp, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temp))
                File.Delete(temp);
        }
    }
}
