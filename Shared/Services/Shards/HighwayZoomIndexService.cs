using System.Text.Json;
using Azure;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using Shared.Models;

namespace Shared.Services.Shards;

public class HighwayZoomIndexService(
    BlobContainerClient container,
    IShardRepository shardRepository,
    ILogger<HighwayZoomIndexService> logger,
    int canonicalZoom = 12)
{
    private const int IndexVersion = 3;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private readonly BlobContainerClient _container = container;
    private readonly IShardRepository _shardRepository = shardRepository;
    private readonly ILogger<HighwayZoomIndexService> _logger = logger;
    private readonly int _canonicalZoom = canonicalZoom;

    public async Task<HighwayTileShardSelection> GetShardKeysAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        if (z == _canonicalZoom)
            return new HighwayTileShardSelection(true, [(x, y)]);

        if (z > _canonicalZoom)
        {
            var scale = 1 << (z - _canonicalZoom);
            return new HighwayTileShardSelection(true, [(x / scale, y / scale)]);
        }

        var cached = await TryReadIndexAsync(z, x, y, cancellationToken);
        if (cached is not null)
            return cached;

        var built = await BuildAndPersistIndexAsync(z, x, y, cancellationToken);
        return built;
    }

    public async Task UpdateIndexesForShardAsync(int shardX, int shardY, Shard shard, CancellationToken cancellationToken = default)
    {
        if (shard is null)
            throw new ArgumentNullException(nameof(shard));

        for (var z = 0; z < _canonicalZoom; z++)
        {
            var shift = _canonicalZoom - z;
            var x = shardX >> shift;
            var y = shardY >> shift;
            var blob = _container.GetBlobClient(GetIndexBlobPath(z, x, y));

            var index = await TryReadIndexModelAsync(blob, z, x, y, cancellationToken);
            if (index is null)
                continue;

            var shouldInclude = shard.Owned.Any(feature => HighwayZoomRules.ShouldKeepFeature(feature, z));
            var existingIndex = index.Shards.FindIndex(reference => reference.X == shardX && reference.Y == shardY);
            var changed = false;

            if (shouldInclude && existingIndex < 0)
            {
                index.Shards.Add(new HighwayTileShardRef(shardX, shardY));
                changed = true;
            }
            else if (!shouldInclude && existingIndex >= 0)
            {
                index.Shards.RemoveAt(existingIndex);
                changed = true;
            }

            if (!changed)
                continue;

            var payload = JsonSerializer.SerializeToUtf8Bytes(index, JsonOptions);
            await blob.UploadAsync(BinaryData.FromBytes(payload), overwrite: true, cancellationToken);

            _logger.LogInformation(
                "Updated highway shard index z{Z}/{X}/{Y} with shard {ShardX}/{ShardY}. Included={Included}.",
                z,
                x,
                y,
                shardX,
                shardY,
                shouldInclude);
        }
    }

    private async Task<HighwayTileShardSelection> BuildAndPersistIndexAsync(int z, int x, int y, CancellationToken cancellationToken)
    {
        var candidateShards = BlobTileService.GetIntersectingShardKeys(z, x, y, _canonicalZoom);
        var retained = new List<(int x, int y)>();
        var isComplete = true;

        foreach (var key in candidateShards)
        {
            var shard = await _shardRepository.TryGetShardAsync(_canonicalZoom, key.x, key.y, cancellationToken);
            if (shard is null)
            {
                isComplete = false;
                continue;
            }

            if (shard.Owned.Any(feature => HighwayZoomRules.ShouldKeepFeature(feature, z)))
                retained.Add(key);
        }

        var index = new HighwayTileShardIndex(
            IndexVersion,
            z,
            x,
            y,
            isComplete,
            [.. retained.Select(k => new HighwayTileShardRef(k.x, k.y))]);

        // Persist both complete and incomplete selections to avoid re-scanning all candidate
        // canonical shards on every low-zoom request. Incomplete indexes are safe to cache
        // because UpdateIndexesForShardAsync amends existing index blobs as shards materialize.
        var blob = _container.GetBlobClient(GetIndexBlobPath(z, x, y));
        var payload = JsonSerializer.SerializeToUtf8Bytes(index, JsonOptions);
        await blob.UploadAsync(BinaryData.FromBytes(payload), overwrite: true, cancellationToken);

        _logger.LogInformation(
            "Built highway shard index for z{Z}/{X}/{Y}. Referenced shards: {ShardCount}. IsComplete: {IsComplete}.",
            z, x, y, retained.Count, isComplete);

        return new HighwayTileShardSelection(isComplete, retained);
    }

    private async Task<HighwayTileShardSelection?> TryReadIndexAsync(int z, int x, int y, CancellationToken cancellationToken)
    {
        var blob = _container.GetBlobClient(GetIndexBlobPath(z, x, y));
        var model = await TryReadIndexModelAsync(blob, z, x, y, cancellationToken);
        if (model is null)
            return null;

        return new HighwayTileShardSelection(
            model.IsComplete,
            [.. model.Shards.Select(shard => (shard.X, shard.Y)).Distinct()]);
    }

    private async Task<HighwayTileShardIndex?> TryReadIndexModelAsync(BlobClient blob, int z, int x, int y, CancellationToken cancellationToken)
    {
        try
        {
            var download = await blob.DownloadContentAsync(cancellationToken);
            var model = JsonSerializer.Deserialize<HighwayTileShardIndex>(download.Value.Content.ToArray(), JsonOptions);
            if (model is null || model.Version != IndexVersion || model.Zoom != z || model.X != x || model.Y != y)
                return null;

            return model;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ignoring invalid highway shard index for z{Z}/{X}/{Y}.", z, x, y);
            return null;
        }
    }

    internal static string GetIndexBlobPath(int z, int x, int y) => $"index/{z}/{x}/{y}.json";
}

public sealed record HighwayTileShardSelection(bool IsComplete, IReadOnlyList<(int x, int y)> Shards);
public sealed record HighwayTileShardIndex(int Version, int Zoom, int X, int Y, bool IsComplete, List<HighwayTileShardRef> Shards);
public sealed record HighwayTileShardRef(int X, int Y);
