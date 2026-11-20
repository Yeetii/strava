using Microsoft.Extensions.Logging;
using Stigar.Core.Geo;
using Stigar.Core.Models;
using Stigar.Core.Services;
using BAMCIS.GeoJSON;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Shards;
using Stigar.Core.Jobs;

namespace Stigar.Workers;

public class VisitedPathsWorker(
    ILogger<VisitedPathsWorker> _logger,
    IActivityRepository _activityRepository,
    IActivityVisitedPathRepository _visitedPathRepository,
    ShardFeatureClient _highwaysShardFeatureClient,
    UserSyncStatusService _userSyncStatusService) : IVisitedPathsJob
{
    // 50m ≈ 0.00045° – use a slightly larger threshold for a rough but fast check
    private const double ProximityThresholdDegrees = 0.0005;

    // Grid cell size matches the proximity threshold for O(1) lookups
    private const double GridCellSize = ProximityThresholdDegrees;

    internal sealed record PathSectionCells(
        IReadOnlyList<(int LatCell, int LngCell)> First,
        IReadOnlyList<(int LatCell, int LngCell)> Middle,
        IReadOnlyList<(int LatCell, int LngCell)> Last);

    internal sealed record PathGridIndex(
        IReadOnlyList<Feature> Paths,
        Dictionary<(int LatCell, int LngCell), List<int>> CellToPathIndices,
        IReadOnlyList<PathSectionCells?> SectionCellsByPathIndex);

    private record ActivitySlim(string Id, string UserId, string? Polyline, string? SummaryPolyline);

    public async Task ExecuteAsync(ActivityIdJob job, CancellationToken cancellationToken)
    {
        var activityId = job.ActivityId;
        var activities = await FetchActivitySlims([activityId], cancellationToken);
        var activitiesList = activities
            .Where(a => !string.IsNullOrWhiteSpace(a.Polyline ?? a.SummaryPolyline))
            .ToList();
        var activitiesById = activitiesList.ToDictionary(a => a.Id);

        // Decode polylines once and reuse
        var decodedActivities = activitiesList.ToDictionary(
            a => a.Id,
            a => GeoSpatialFunctions.DecodePolyline(a.Polyline ?? a.SummaryPolyline).ToList());

        var nearbyPaths = (await FetchNearbyPaths(decodedActivities)).ToList();
        var pathGridIndex = BuildPathGridIndex(nearbyPaths);

        await ProcessJob(activityId, activitiesById, decodedActivities, pathGridIndex, job.Epoch, cancellationToken);
    }

    private async Task ProcessJob(
        string activityId,
        Dictionary<string, ActivitySlim> activitiesById,
        Dictionary<string, List<Coordinate>> decodedActivities,
        PathGridIndex pathGridIndex,
        long epoch,
        CancellationToken cancellationToken)
    {
        activitiesById.TryGetValue(activityId, out var activity);

        if (activity == null || !decodedActivities.TryGetValue(activityId, out var activityPoints) || activityPoints.Count == 0)
        {
            _logger.LogInformation("Skipping activity {ActivityId} since it has no geodata", activityId);
            if (activity != null)
            {
                await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedPaths, epoch, cancellationToken);
            }
            return;
        }

        var activityGrid = BuildSpatialGrid(activityPoints);
        var visitedPaths = FindVisitedPaths(activityGrid, pathGridIndex).ToList();
        var visitedPathsByOsmId = visitedPaths
            .Select(path => (Path: path, OsmHighwayId: GetOsmHighwayId(path)))
            .Where(match => !string.IsNullOrWhiteSpace(match.OsmHighwayId))
            .GroupBy(match => match.OsmHighwayId!, StringComparer.Ordinal)
            .Select(group => (group.First().Path, OsmHighwayId: group.Key))
            .ToList();

        _logger.LogInformation(
            "Activity {ActivityId} visits {PathCount} paths ({OsmPathCount} by canonical osmId)",
            activityId,
            visitedPaths.Count,
            visitedPathsByOsmId.Count);

        if (visitedPathsByOsmId.Count == 0)
        {
            // The junction table is the sole record of which paths an activity visited.
            await _visitedPathRepository.RemoveByActivity(activity.Id, cancellationToken);
            await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedPaths, epoch, cancellationToken);
            return;
        }

        var paths = visitedPathsByOsmId
            .Select(pathMatch =>
            {
                var pathFeature = pathMatch.Path;
                var featureCentroid = GeometryCentroidHelper.GetCentroid(pathFeature.Geometry);
                var (featureTileX, featureTileY) = SlippyTileCalculator.WGS84ToTileIndex(featureCentroid, _highwaysShardFeatureClient.CanonicalZoom);

                return new VisitedPathInput(
                    activity.Id,
                    activity.UserId,
                    pathMatch.OsmHighwayId,
                    featureTileX,
                    featureTileY);
            })
            .ToList();

        // Replace the activity's previous links, then insert the current ones. AddPath
        // carries the name/type/tile columns; the writes are idempotent, and the FK
        // cascade removes rows when the activity itself is deleted.
        await _visitedPathRepository.RemoveByActivity(activity.Id, cancellationToken);
        await _visitedPathRepository.AddRange(paths, cancellationToken);
        await _userSyncStatusService.TryMarkActivityStageProcessed(activity.UserId, activity.Id, ActivitySyncStage.VisitedPaths, epoch, cancellationToken);
    }

    internal static HashSet<(int, int)> BuildSpatialGrid(List<Coordinate> points)
    {
        var grid = new HashSet<(int, int)>(points.Count);
        foreach (var p in points)
            grid.Add(((int)Math.Floor(p.Lat / GridCellSize), (int)Math.Floor(p.Lng / GridCellSize)));
        return grid;
    }

    internal static PathGridIndex BuildPathGridIndex(IReadOnlyList<Feature> paths)
    {
        var cellToPathIndices = new Dictionary<(int LatCell, int LngCell), List<int>>();
        var sectionCellsByPathIndex = new PathSectionCells?[paths.Count];

        for (var i = 0; i < paths.Count; i++)
        {
            var path = paths[i];
            if (path.Geometry is not LineString line)
                continue;

            if (!line.Coordinates.Any())
                continue;

            sectionCellsByPathIndex[i] = BuildSectionCells(line.Coordinates);

            // Avoid adding the same path multiple times for duplicate coordinates in the same cell.
            var touchedCells = new HashSet<(int LatCell, int LngCell)>();
            foreach (var p in line.Coordinates)
            {
                var cell = ((int)Math.Floor(p.Latitude / GridCellSize), (int)Math.Floor(p.Longitude / GridCellSize));
                if (!touchedCells.Add(cell))
                    continue;

                if (!cellToPathIndices.TryGetValue(cell, out var pathIndices))
                {
                    pathIndices = new List<int>();
                    cellToPathIndices[cell] = pathIndices;
                }

                pathIndices.Add(i);
            }
        }

        return new PathGridIndex(paths, cellToPathIndices, sectionCellsByPathIndex);
    }

    internal static IEnumerable<Feature> FindVisitedPaths(HashSet<(int, int)> activityGrid, PathGridIndex index)
    {
        var visitedPathIndices = new HashSet<int>();

        foreach (var (latCell, lngCell) in activityGrid)
        {
            // Check the cell and all 8 neighbors to account for points near cell boundaries
            for (int dLat = -1; dLat <= 1; dLat++)
            {
                for (int dLng = -1; dLng <= 1; dLng++)
                {
                    if (!index.CellToPathIndices.TryGetValue((latCell + dLat, lngCell + dLng), out var candidates))
                        continue;

                    foreach (var pathIndex in candidates)
                    {
                        if (!visitedPathIndices.Add(pathIndex))
                            continue;

                        if (!PathIsVisitedBySections(activityGrid, index.SectionCellsByPathIndex[pathIndex]))
                            continue;

                        yield return index.Paths[pathIndex];
                    }
                }
            }
        }
    }

    private static bool PathIsVisitedBySections(HashSet<(int, int)> activityGrid, PathSectionCells? sectionCells)
    {
        if (sectionCells is null)
            return false;

        if (!SectionIsVisited(activityGrid, sectionCells.First))
            return false;
        if (!SectionIsVisited(activityGrid, sectionCells.Middle))
            return false;
        if (!SectionIsVisited(activityGrid, sectionCells.Last))
            return false;

        return true;
    }

    private static bool SectionIsVisited(HashSet<(int, int)> activityGrid, IReadOnlyList<(int LatCell, int LngCell)> sectionCells)
    {
        if (sectionCells.Count == 0)
            return true;

        foreach (var (latCell, lngCell) in sectionCells)
        {
            for (var dLat = -1; dLat <= 1; dLat++)
            {
                for (var dLng = -1; dLng <= 1; dLng++)
                {
                    if (activityGrid.Contains((latCell + dLat, lngCell + dLng)))
                        return true;
                }
            }
        }

        return false;
    }

    private static PathSectionCells BuildSectionCells(IEnumerable<Position> coordinates)
    {
        var points = coordinates.ToList();
        var first = new HashSet<(int LatCell, int LngCell)>();
        var middle = new HashSet<(int LatCell, int LngCell)>();
        var last = new HashSet<(int LatCell, int LngCell)>();

        var total = points.Count;
        for (var index = 0; index < total; index++)
        {
            var point = points[index];
            var cell = ((int)Math.Floor(point.Latitude / GridCellSize), (int)Math.Floor(point.Longitude / GridCellSize));
            var section = (index * 3) / total;

            if (section <= 0)
                first.Add(cell);
            else if (section == 1)
                middle.Add(cell);
            else
                last.Add(cell);
        }

        return new PathSectionCells([.. first], [.. middle], [.. last]);
    }

    private async Task<List<ActivitySlim>> FetchActivitySlims(List<string> ids, CancellationToken cancellationToken)
    {
        var activities = await _activityRepository.GetByIds(ids, cancellationToken);
        return activities
            .Select(activity => new ActivitySlim(activity.Id, activity.UserId, activity.Polyline, activity.SummaryPolyline))
            .ToList();
    }

    private async Task<IEnumerable<Feature>> FetchNearbyPaths(Dictionary<string, List<Coordinate>> decodedActivities)
    {
        var shardZoom = _highwaysShardFeatureClient.CanonicalZoom;
        var tileIndices = new HashSet<(int x, int y)>();
        foreach (var points in decodedActivities.Values)
        {
            foreach (var tile in SlippyTileCalculator.TileIndicesByLine(points, shardZoom))
                tileIndices.Add(tile);
        }

        var paths = (await _highwaysShardFeatureClient.GetFeaturesForShards(tileIndices)).ToList();
        _logger.LogInformation("Found {Count} nearby paths", paths.Count);
        return paths;
    }

    private static string? GetOsmHighwayId(Feature pathFeature)
    {
        if (!pathFeature.Properties.TryGetValue("osmId", out var osmId))
            return null;

        var value = osmId?.ToString() as string;
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }

}
