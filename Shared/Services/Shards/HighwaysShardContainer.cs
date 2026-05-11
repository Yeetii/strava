using Azure.Storage.Blobs;

namespace Shared.Services.Shards;

public sealed class HighwaysShardContainer(BlobContainerClient client)
{
    public BlobContainerClient Client { get; } = client;
}
