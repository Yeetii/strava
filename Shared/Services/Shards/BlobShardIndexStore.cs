using Azure;
using Azure.Storage.Blobs;

namespace Shared.Services.Shards;

/// <summary>
/// Blob-backed <see cref="IShardIndexStore"/>. Preserves the existing Azure behavior
/// while hosts are still selecting the Blob shard stack; T4 switches the selection to
/// the filesystem store without touching <see cref="HighwayZoomIndexService"/>.
/// </summary>
public sealed class BlobShardIndexStore(BlobContainerClient container) : IShardIndexStore
{
    public async Task<byte[]?> TryReadAsync(string path, CancellationToken cancellationToken = default)
    {
        try
        {
            var download = await container.GetBlobClient(path).DownloadContentAsync(cancellationToken);
            return download.Value.Content.ToArray();
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task WriteAsync(string path, byte[] content, CancellationToken cancellationToken = default)
        => await container.GetBlobClient(path).UploadAsync(
            BinaryData.FromBytes(content), overwrite: true, cancellationToken);

    public async Task<DateTimeOffset?> TryGetLastModifiedAsync(string path, CancellationToken cancellationToken = default)
    {
        try
        {
            var properties = await container.GetBlobClient(path).GetPropertiesAsync(cancellationToken: cancellationToken);
            return properties.Value.LastModified;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }
}
