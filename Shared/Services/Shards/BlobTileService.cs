using System.IO.Compression;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Shared.Geo;
using Shared.Models;

namespace Shared.Services.Shards;

public class BlobTileService(
    ShardFeatureClient featureClient,
    HighwayZoomIndexService zoomIndexService,
    ILogger<BlobTileService> logger,
    int shardZoom = 12)
{
    private readonly ShardFeatureClient _featureClient = featureClient;
    private readonly HighwayZoomIndexService _zoomIndexService = zoomIndexService;
    private readonly ILogger<BlobTileService> _logger = logger;
    private readonly int _shardZoom = shardZoom;
    private const double ClipTolerance = 1e-10;
    private const int ShardSampleSize = 20;

    public async Task<HighwayTileBuildResult> BuildTileAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        var selection = await _zoomIndexService.GetShardKeysAsync(z, x, y, cancellationToken);
        var shardKeys = selection.Shards;
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var features = await _featureClient.GetFeaturesForShards(shardKeys, cancellationToken);
            var filtered = FilterByZoom(features, z);
            var clipped = ClipToTileBounds(filtered, z, x, y);
            var simplified = SimplifyByZoom(clipped, z);
            var pbf = MvtTileEncoder.EncodeLayer("highways", simplified, z, x, y);
            return new HighwayTileBuildResult(Gzip(pbf), selection.IsComplete);
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                ex,
                "Cancelled highway tile build for z{Z}/{X}/{Y} (shardZoom={ShardZoom}, shardCount={ShardCount}, shardSample={ShardSample}, elapsedMs={ElapsedMs})",
                z,
                x,
                y,
                _shardZoom,
                shardKeys.Count,
                FormatShardSample(shardKeys),
                stopwatch.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed highway tile build for z{Z}/{X}/{Y} (shardZoom={ShardZoom}, shardCount={ShardCount}, shardSample={ShardSample}, elapsedMs={ElapsedMs})",
                z,
                x,
                y,
                _shardZoom,
                shardKeys.Count,
                FormatShardSample(shardKeys),
                stopwatch.ElapsedMilliseconds);
            throw;
        }
    }

    public async Task<HighwayTileBuildResult> RefreshTileAsync(int z, int x, int y, CancellationToken cancellationToken = default)
    {
        var selection = await _zoomIndexService.GetShardKeysAsync(z, x, y, cancellationToken);
        var shardKeys = selection.Shards;
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await _featureClient.RefreshShards(shardKeys, cancellationToken);
            return await BuildTileAsync(z, x, y, cancellationToken);
        }
        catch (Exception ex) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                ex,
                "Cancelled highway tile refresh for z{Z}/{X}/{Y} (shardZoom={ShardZoom}, shardCount={ShardCount}, shardSample={ShardSample}, elapsedMs={ElapsedMs})",
                z,
                x,
                y,
                _shardZoom,
                shardKeys.Count,
                FormatShardSample(shardKeys),
                stopwatch.ElapsedMilliseconds);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed highway tile refresh for z{Z}/{X}/{Y} (shardZoom={ShardZoom}, shardCount={ShardCount}, shardSample={ShardSample}, elapsedMs={ElapsedMs})",
                z,
                x,
                y,
                _shardZoom,
                shardKeys.Count,
                FormatShardSample(shardKeys),
                stopwatch.ElapsedMilliseconds);
            throw;
        }
    }

    private static string FormatShardSample(IReadOnlyList<(int x, int y)> shardKeys)
    {
        if (shardKeys.Count == 0)
            return string.Empty;

        var sample = shardKeys
            .Take(ShardSampleSize)
            .Select(k => $"{k.x}/{k.y}");
        var formatted = string.Join(",", sample);
        return shardKeys.Count > ShardSampleSize
            ? $"{formatted}...(+{shardKeys.Count - ShardSampleSize} more)"
            : formatted;
    }

    internal static IReadOnlyList<(int x, int y)> GetIntersectingShardKeys(int z, int x, int y, int shardZoom)
        => Geo.SlippyTileCalculator.GetIntersectingTileKeys(z, x, y, shardZoom);

    internal static IEnumerable<Feature> FilterByZoom(IEnumerable<Feature> features, int zoom)
        => features.Where(feature => HighwayZoomRules.ShouldKeepFeature(feature, zoom));

    internal static IEnumerable<Feature> SimplifyByZoom(IEnumerable<Feature> features, int zoom)
    {
        var epsilon = HighwayZoomRules.GetSimplificationEpsilon(zoom);
        if (epsilon <= 0)
            return features;

        return features
            .Select(feature => SimplifyFeature(feature, epsilon))
            .OfType<Feature>();
    }

    private static Feature? SimplifyFeature(Feature feature, double epsilon)
    {
        if (feature.Geometry is not LineString line)
            return feature;

        var coordinates = line.Coordinates
            .Select(position => new Coordinate(position.Longitude, position.Latitude))
            .ToList();

        var simplified = GeometryDecimator.SimplifyTrack(coordinates, epsilon);
        if (simplified.Count < 2)
            return null;

        var simplifiedLine = new LineString(
            simplified.Select(point => new Position(point.Lng, point.Lat)).ToList());

        return new Feature(simplifiedLine, feature.Properties, null, feature.Id);
    }

    internal static IEnumerable<Feature> ClipToTileBounds(IEnumerable<Feature> features, int z, int x, int y)
    {
        var (southWest, northEast) = Geo.SlippyTileCalculator.TileIndexToWGS84(x, y, z);
        foreach (var feature in features)
        {
            switch (feature.Geometry)
            {
                case Point point:
                    if (point.Coordinates.Longitude >= southWest.Lng
                        && point.Coordinates.Longitude <= northEast.Lng
                        && point.Coordinates.Latitude >= southWest.Lat
                        && point.Coordinates.Latitude <= northEast.Lat)
                        yield return feature;
                    break;

                case LineString line:
                    var clippedFragments = ClipLineToBounds(line, southWest.Lng, southWest.Lat, northEast.Lng, northEast.Lat);
                    if (clippedFragments.Count == 1)
                    {
                        var fragment = clippedFragments[0];
                        if (fragment.Count >= 2)
                            yield return new Feature(new LineString(fragment), feature.Properties, null, feature.Id);
                        break;
                    }

                    for (var fragmentIndex = 0; fragmentIndex < clippedFragments.Count; fragmentIndex++)
                    {
                        var fragment = clippedFragments[fragmentIndex];
                        if (fragment.Count < 2)
                            continue;

                        yield return new Feature(
                            new LineString(fragment),
                            feature.Properties,
                            null,
                            feature.Id);
                    }
                    break;
            }
        }
    }

    private static List<List<Position>> ClipLineToBounds(LineString line, double minX, double minY, double maxX, double maxY)
    {
        var fragments = new List<List<Position>>();
        List<Position>? currentFragment = null;
        Position? lastPoint = null;

        foreach (var segment in line.Coordinates.Zip(line.Coordinates.Skip(1)))
        {
            if (!TryClipSegment(segment.First, segment.Second, minX, minY, maxX, maxY, out var start, out var end))
            {
                currentFragment = null;
                lastPoint = null;
                continue;
            }

            if (currentFragment is null
                || lastPoint is null
                || lastPoint.Longitude != start.Longitude
                || lastPoint.Latitude != start.Latitude)
            {
                currentFragment = [start];
                fragments.Add(currentFragment);
            }

            currentFragment.Add(end);
            lastPoint = end;
        }

        return fragments;
    }

    private static bool TryClipSegment(Position p0, Position p1, double minX, double minY, double maxX, double maxY, out Position c0, out Position c1)
    {
        var x0 = p0.Longitude;
        var y0 = p0.Latitude;
        var x1 = p1.Longitude;
        var y1 = p1.Latitude;
        var code0 = ComputeCode(x0, y0, minX, minY, maxX, maxY);
        var code1 = ComputeCode(x1, y1, minX, minY, maxX, maxY);

        while (true)
        {
            if ((code0 | code1) == 0)
            {
                c0 = new Position(x0, y0);
                c1 = new Position(x1, y1);
                return true;
            }

            if ((code0 & code1) != 0)
            {
                c0 = default!;
                c1 = default!;
                return false;
            }

            var outCode = code0 != 0 ? code0 : code1;
            double x;
            double y;
            if ((outCode & 8) != 0)
            {
                if (Math.Abs(y1 - y0) < ClipTolerance)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                x = x0 + ((x1 - x0) * (maxY - y0) / (y1 - y0));
                y = maxY;
            }
            else if ((outCode & 4) != 0)
            {
                if (Math.Abs(y1 - y0) < ClipTolerance)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                x = x0 + ((x1 - x0) * (minY - y0) / (y1 - y0));
                y = minY;
            }
            else if ((outCode & 2) != 0)
            {
                if (Math.Abs(x1 - x0) < ClipTolerance)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                y = y0 + ((y1 - y0) * (maxX - x0) / (x1 - x0));
                x = maxX;
            }
            else
            {
                if (Math.Abs(x1 - x0) < ClipTolerance)
                {
                    c0 = default!;
                    c1 = default!;
                    return false;
                }
                y = y0 + ((y1 - y0) * (minX - x0) / (x1 - x0));
                x = minX;
            }

            if (outCode == code0)
            {
                x0 = x;
                y0 = y;
                code0 = ComputeCode(x0, y0, minX, minY, maxX, maxY);
            }
            else
            {
                x1 = x;
                y1 = y;
                code1 = ComputeCode(x1, y1, minX, minY, maxX, maxY);
            }
        }
    }

    private static int ComputeCode(double x, double y, double minX, double minY, double maxX, double maxY)
    {
        var code = 0;
        if (x < minX) code |= 1;
        else if (x > maxX) code |= 2;
        if (y < minY) code |= 4;
        else if (y > maxY) code |= 8;
        return code;
    }

    private static byte[] Gzip(byte[] payload)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
            gzip.Write(payload, 0, payload.Length);
        return output.ToArray();
    }
}

public sealed record HighwayTileBuildResult(byte[] Payload, bool IsComplete);
