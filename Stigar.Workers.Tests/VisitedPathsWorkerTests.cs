using Stigar.Core.Jobs;
using BAMCIS.GeoJSON;
using Hangfire;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Shards;

namespace Stigar.Workers.Tests;

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

    [Fact]
    public async Task Run_WritesVisitedPathLinksUsingPostgresRepositories()
    {
        const string userId = "user-1";
        const string activityId = "activity-1";

        var activity = new Activity
        {
            Id = activityId,
            UserId = userId,
            Name = "Morning Run",
            SportType = SportTypes.RUN,
            StartDate = DateTime.UtcNow,
            StartDateLocal = DateTime.UtcNow,
            Polyline = "_p~iF~ps|U_ulLnnqC_mqNvxq`@`",
            ProcessingStatus = new ActivityProcessingStatus { VisitedPathsEpoch = 1 }
        };

        var path = new Feature(
            new LineString(
            [
                new Position(-120.2, 38.5),
                new Position(-120.95, 40.7),
                new Position(-126.453, 43.252)
            ]),
            new Dictionary<string, dynamic>
            {
                ["osmId"] = "way/123",
                ["name"] = "Test Road",
                ["highway"] = "residential"
            },
            null,
            new FeatureId("path-1"));

        var activities = new Mock<IActivityRepository>();
        activities
            .Setup(r => r.GetByIds(It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([activity]);
        activities
            .Setup(r => r.GetById(activityId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);

        var shards = new Mock<ShardFeatureClient>(
            Mock.Of<IShardRepository>(),
            NullLogger<ShardFeatureClient>.Instance,
            12);
        shards
            .Setup(s => s.GetFeaturesForShards(
                It.IsAny<IEnumerable<(int x, int y)>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync([path]);

        var visitedPaths = new Mock<IActivityVisitedPathRepository>();
        var users = new Mock<IUserRepository>();
        var syncStatus = new UserSyncStatusService(users.Object, activities.Object);

        var worker = new VisitedPathsWorker(
            NullLogger<VisitedPathsWorker>.Instance,
            activities.Object,
            visitedPaths.Object,
            shards.Object,
            syncStatus);

        await worker.ExecuteAsync(new ActivityIdJob { ActivityId = activityId }, CancellationToken.None);

        visitedPaths.Verify(r => r.RemoveByActivity(activityId, It.IsAny<CancellationToken>()), Times.Once);
        visitedPaths.Verify(
            r => r.AddRange(
                It.Is<IEnumerable<VisitedPathInput>>(paths => paths.Count() == 1
                    && paths.First().ActivityId == activityId
                    && paths.First().UserId == userId
                    && paths.First().OsmHighwayId == "way/123"
                    && paths.First().TileX >= 0
                    && paths.First().TileY >= 0),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    private static Feature CreatePath(string id, params Position[] points)
        => new(
            new LineString(points),
            new Dictionary<string, dynamic>(),
            null,
            new FeatureId(id));
}
