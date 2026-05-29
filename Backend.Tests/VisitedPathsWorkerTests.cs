using BAMCIS.GeoJSON;
using Shared.Models;

namespace Backend.Tests;

public class VisitedPathsWorkerTests
{
    [Fact]
    public void FindVisitedPaths_MatchesPathsInNeighborCells()
    {
        var activityGrid = VisitedPathsWorker.BuildSpatialGrid([
            new Coordinate(0.00010, 0.00010)
        ]);

        var nearbyPath = CreatePath(
            "nearby",
            new Position(0.00055, 0.00010),
            new Position(0.00060, 0.00015));

        var farPath = CreatePath(
            "far",
            new Position(0.0030, 0.0030),
            new Position(0.0035, 0.0035));

        var index = VisitedPathsWorker.BuildPathGridIndex([nearbyPath, farPath]);
        var visitedIds = VisitedPathsWorker
            .FindVisitedPaths(activityGrid, index)
            .Select(f => f.Id.Value)
            .ToList();

        Assert.Contains("nearby", visitedIds);
        Assert.DoesNotContain("far", visitedIds);
    }

    [Fact]
    public void FindVisitedPaths_ReturnsEachPathOnlyOnce()
    {
        var activityGrid = VisitedPathsWorker.BuildSpatialGrid([
            new Coordinate(0.00010, 0.00010),
            new Coordinate(0.00040, 0.00040)
        ]);

        var duplicateCandidate = CreatePath(
            "dup",
            new Position(0.00010, 0.00010),
            new Position(0.00060, 0.00010),
            new Position(0.00060, 0.00060));

        var secondMatch = CreatePath(
            "second",
            new Position(0.00045, 0.00045),
            new Position(0.00049, 0.00049));

        var index = VisitedPathsWorker.BuildPathGridIndex([duplicateCandidate, secondMatch]);
        var visitedIds = VisitedPathsWorker
            .FindVisitedPaths(activityGrid, index)
            .Select(f => f.Id.Value)
            .ToList();

        Assert.Equal(2, visitedIds.Count);
        Assert.Equal(2, visitedIds.Distinct().Count());
        Assert.Contains("dup", visitedIds);
        Assert.Contains("second", visitedIds);
    }

    [Fact]
    public void BuildPathGridIndex_IgnoresNonLineStringGeometries()
    {
        var pointFeature = new Feature(
            new Point(new Position(0.00010, 0.00010)),
            new Dictionary<string, dynamic>(),
            null,
            new FeatureId("point"));

        var lineFeature = CreatePath(
            "line",
            new Position(0.00010, 0.00010),
            new Position(0.00015, 0.00015));

        var activityGrid = VisitedPathsWorker.BuildSpatialGrid([
            new Coordinate(0.00010, 0.00010)
        ]);

        var index = VisitedPathsWorker.BuildPathGridIndex([pointFeature, lineFeature]);
        var visitedIds = VisitedPathsWorker
            .FindVisitedPaths(activityGrid, index)
            .Select(f => f.Id.Value)
            .ToList();

        Assert.Single(visitedIds);
        Assert.Equal("line", visitedIds[0]);
    }

    [Fact]
    public void FindVisitedPaths_RequiresActivityInAllThreePathThirds()
    {
        var path = CreatePath(
            "thirds",
            new Position(0.00010, 0.00010),
            new Position(0.00015, 0.00015),
            new Position(0.00310, 0.00310),
            new Position(0.00315, 0.00315),
            new Position(0.00610, 0.00610),
            new Position(0.00615, 0.00615));

        var activityGrid = VisitedPathsWorker.BuildSpatialGrid([
            new Coordinate(0.00010, 0.00010),
            new Coordinate(0.00310, 0.00310),
            new Coordinate(0.00610, 0.00610)
        ]);

        var index = VisitedPathsWorker.BuildPathGridIndex([path]);
        var visitedIds = VisitedPathsWorker
            .FindVisitedPaths(activityGrid, index)
            .Select(f => f.Id.Value)
            .ToList();

        Assert.Single(visitedIds);
        Assert.Equal("thirds", visitedIds[0]);
    }

    [Fact]
    public void FindVisitedPaths_DoesNotCountPathWhenAnyThirdHasNoVisit()
    {
        var path = CreatePath(
            "thirds-miss",
            new Position(0.00010, 0.00010),
            new Position(0.00015, 0.00015),
            new Position(0.00310, 0.00310),
            new Position(0.00315, 0.00315),
            new Position(0.00610, 0.00610),
            new Position(0.00615, 0.00615));

        var activityGrid = VisitedPathsWorker.BuildSpatialGrid([
            new Coordinate(0.00010, 0.00010),
            new Coordinate(0.00610, 0.00610)
        ]);

        var index = VisitedPathsWorker.BuildPathGridIndex([path]);
        var visitedIds = VisitedPathsWorker
            .FindVisitedPaths(activityGrid, index)
            .Select(f => f.Id.Value)
            .ToList();

        Assert.Empty(visitedIds);
    }

    private static Feature CreatePath(string id, params Position[] points)
        => new(
            new LineString(points),
            new Dictionary<string, dynamic>(),
            null,
            new FeatureId(id));
}
