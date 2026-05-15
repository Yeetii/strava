using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;

namespace Shared.Services.Shards;

public class HighwayZoomIndexService(
    BlobContainerClient container,
    IShardRepository shardRepository,
    ILogger<HighwayZoomIndexService> logger,
    int canonicalZoom = 12)
{
    private const int IndexVersion = 1;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly BlobContainerClient _container = container;
    private readonly IShardRepository _shardRepository = shardRepository;
    private readonly ILogger<HighwayZoomIndexService> _logger = logger;
    private readonly int _canonicalZoom = canonicalZoom;

    public async Task<IReadOnlyList<(int x, int y)>> GetShardKeysAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z == _canonicalZoom)
            return [(x, y)];

        if (z > _canonicalZoom)
        {
            var scale = 1 << (z - _canonicalZoom);
            return [(x / scale, y / scale)];
        }

        var cached = await TryReadIndexAsync(z, x, y, cancellationToken);
        if (cached.Count > 0)
            return cached;

        var built = await BuildAndPersistIndexAsync(z, x, y, cancellationToken);
        return built;
    }

    private async Task<IReadOnlyList<(int x, int y)>> BuildAndPersistIndexAsync(int z, int x, int y, CancellationToken cancellationToken)
    {
        var candidateShards = BlobTileService.GetIntersectingShardKeys(z, x, y, _canonicalZoom);
        var retained = new List<(int x, int y)>();

        foreach (var key in candidateShards)
        {
            var shard = await _shardRepository.TryGetShardAsync(_canonicalZoom, key.x, key.y, cancellationToken);
            if (shard is not null && shard.Owned.Any(feature => HighwayZoomRules.ShouldKeepFeature(feature, z)))
                retained.Add(key);
        }

        var index = new HighwayTileShardIndex(
            IndexVersion,
            z,
            x,
            y,
            [.. retained.Select(k => new HighwayTileShardRef(k.x, k.y))]);

        var blob = _container.GetBlobClient(GetIndexBlobPath(z, x, y));
        var payload = JsonSerializer.SerializeToUtf8Bytes(index, JsonOptions);
        await blob.UploadAsync(BinaryData.FromBytes(payload), overwrite: true, cancellationToken);

        _logger.LogInformation(
            "Built highway shard index for z{Z}/{X}/{Y}. Referenced shards: {ShardCount}.",
            z, x, y, retained.Count);

        return retained;
    }

    private async Task<IReadOnlyList<(int x, int y)>> TryReadIndexAsync(int z, int x, int y, CancellationToken cancellationToken)
    {
        var blob = _container.GetBlobClient(GetIndexBlobPath(z, x, y));
        try
        {
            var download = await blob.DownloadContentAsync(cancellationToken);
            var model = JsonSerializer.Deserialize<HighwayTileShardIndex>(download.Value.Content.ToArray(), JsonOptions);
            if (model is null || model.Version != IndexVersion || model.Zoom != z || model.X != x || model.Y != y)
                return [];

            return [.. model.Shards.Select(shard => (shard.X, shard.Y)).Distinct()];
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return [];
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ignoring invalid highway shard index for z{Z}/{X}/{Y}.", z, x, y);
            return [];
        }
    }

    internal static string GetIndexBlobPath(int z, int x, int y) => $"index/{z}/{x}/{y}.json";
}

public sealed record HighwayTileShardIndex(int Version, int Zoom, int X, int Y, List<HighwayTileShardRef> Shards);
public sealed record HighwayTileShardRef(int X, int Y);
