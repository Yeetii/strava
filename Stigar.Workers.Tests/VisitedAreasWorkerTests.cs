using Stigar.Core.Jobs;
using BAMCIS.GeoJSON;
using Hangfire;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Stigar.Core.Constants;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;

namespace Stigar.Workers.Tests;

public class VisitedAreasWorkerTests
{
    [Fact]
    public void GetBoundaryLookupPolyline_PrefersFullPolylineOverSummaryPolyline()
    {
        var activity = new Activity
        {
            Id = "activity-1",
            UserId = "user-1",
            Name = "Morning Run",
            SportType = SportTypes.RUN,
            StartDate = DateTime.UtcNow,
            StartDateLocal = DateTime.UtcNow,
            Polyline = "full-polyline",
            SummaryPolyline = "summary-polyline"
        };

        var polyline = VisitedAreasWorker.GetBoundaryLookupPolyline(activity);

        Assert.Equal("full-polyline", polyline);
    }

    [Fact]
    public void ValidateAdminRegionCandidates_ReturnsEmptyWhenNoBoundaryIntersectsActivity()
    {
        var summary = CreateSummary("boundary-1", "Summary name");
        var boundary = CreateBoundary("boundary-1", CreateSquare(1, 1, 2, 2));
        var activityPoints = new[] { new Coordinate(0, 0), new Coordinate(0.2, 0.2) };

        var validated = VisitedAreasWorker.ValidateAdminRegionCandidates(
            [summary],
            new Dictionary<string, StoredFeature> { [summary.Id] = boundary },
            activityPoints);

        Assert.Empty(validated);
    }

    [Fact]
    public void ValidateAdminRegionCandidates_MergesBoundaryPropertiesForIntersectingRegions()
    {
        var summary = CreateSummary("boundary-1", "Summary name");
        var boundary = CreateBoundary("boundary-1", CreateSquare(-1, -1, 1, 1));
        boundary.Properties["countryCode"] = "NO";
        boundary.Properties["name"] = "Boundary name";
        var activityPoints = new[] { new Coordinate(0, 0), new Coordinate(0.1, 0.1) };

        var validated = VisitedAreasWorker.ValidateAdminRegionCandidates(
            [summary],
            new Dictionary<string, StoredFeature> { [summary.Id] = boundary },
            activityPoints);

        var region = Assert.Single(validated);
        Assert.Equal(summary.Id, region.Id);
        Assert.Equal("Boundary name", region.Properties["name"]);
        Assert.Equal("NO", region.Properties["countryCode"]);
    }

    [Fact]
    public async Task Run_WritesVisitedAreaLinksUsingPostgresRepositories()
    {
        const string userId = "user-1";
        const string activityId = "activity-1";
        const string areaId = "prot-1";

        var activity = new Activity
        {
            Id = activityId,
            UserId = userId,
            Name = "Morning Run",
            SportType = SportTypes.RUN,
            StartDate = DateTime.UtcNow,
            StartDateLocal = DateTime.UtcNow,
            Polyline = "_p~iF~ps|U_ulLnnqC_mqNvxq`@`",
            ProcessingStatus = new ActivityProcessingStatus { VisitedAreas = true }
        };

        var area = new StoredFeature
        {
            Id = areaId,
            FeatureId = areaId,
            Kind = FeatureKinds.ProtectedArea,
            X = 0,
            Y = 0,
            Zoom = 8,
            Geometry = CreateSquare(-127, 37, -119, 44),
            Properties = new Dictionary<string, dynamic>
            {
                ["name"] = "Test Park",
                ["areaType"] = "national_park"
            }
        };

        var activities = new Mock<IActivityRepository>();
        activities
            .Setup(r => r.GetByIds(It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([activity]);
        activities
            .Setup(r => r.GetById(activityId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);

        var protectedAreas = new Mock<IProtectedAreaRepository>();
        protectedAreas
            .Setup(r => r.FindContainingPoint(It.IsAny<Coordinate>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([area]);

        var adminBoundaries = new Mock<IAdminBoundaryRepository>();
        adminBoundaries
            .Setup(r => r.FindSummariesContainingAnyPoint(
                It.IsAny<IEnumerable<Coordinate>>(),
                It.IsAny<int?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync([]);

        var visitedAreas = new Mock<IActivityVisitedAreaRepository>();
        var users = new Mock<IUserRepository>();
        var syncStatus = new UserSyncStatusService(users.Object, activities.Object);

        var worker = new VisitedAreasWorker(
            NullLogger<VisitedAreasWorker>.Instance,
            activities.Object,
            visitedAreas.Object,
            protectedAreas.Object,
            adminBoundaries.Object,
            syncStatus,
            CreateIngestionMock().Object);

        await worker.ExecuteAsync(new ActivityIdJob { ActivityId = activityId }, CancellationToken.None);

        protectedAreas.Verify(
            r => r.FindContainingPoint(It.IsAny<Coordinate>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
        visitedAreas.Verify(r => r.RemoveByActivity(activityId, It.IsAny<CancellationToken>()), Times.Once);
        visitedAreas.Verify(
            r => r.AddRange(
                It.Is<IEnumerable<VisitedAreaLink>>(links => links.Count() == 1
                    && links.First().ActivityId == activityId
                    && links.First().UserId == userId
                    && links.First().AreaId == areaId),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Run_EnsuresFeaturesBeforeSpatialQueries()
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
            ProcessingStatus = new ActivityProcessingStatus { VisitedAreas = true }
        };

        var area = new StoredFeature
        {
            Id = "prot-1",
            FeatureId = "prot-1",
            Kind = FeatureKinds.ProtectedArea,
            X = 0,
            Y = 0,
            Zoom = 8,
            Geometry = CreateSquare(-127, 37, -119, 44),
            Properties = new Dictionary<string, dynamic>
            {
                ["name"] = "Test Park",
                ["areaType"] = "national_park"
            }
        };

        var callOrder = new List<string>();

        var activities = new Mock<IActivityRepository>();
        activities
            .Setup(r => r.GetByIds(It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([activity]);
        activities
            .Setup(r => r.GetById(activityId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);

        var ingestion = CreateIngestionMock();
        ingestion
            .Setup(s => s.EnsureProtectedAreasAsync(It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("ingestProtected"))
            .ReturnsAsync(new OsmIngestionResult(1, 1, 1, 0));
        ingestion
            .Setup(s => s.EnsureAdminBoundariesAsync(
                It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("ingestAdmin"))
            .ReturnsAsync(new OsmIngestionResult(1, 1, 1, 0));

        var protectedAreas = new Mock<IProtectedAreaRepository>();
        protectedAreas
            .Setup(r => r.FindContainingPoint(It.IsAny<Coordinate>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("queryProtected"))
            .ReturnsAsync([area]);

        var adminBoundaries = new Mock<IAdminBoundaryRepository>();
        adminBoundaries
            .Setup(r => r.FindSummariesContainingAnyPoint(
                It.IsAny<IEnumerable<Coordinate>>(),
                It.IsAny<int?>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("queryAdmin"))
            .ReturnsAsync([]);

        var users = new Mock<IUserRepository>();
        var syncStatus = new UserSyncStatusService(users.Object, activities.Object);
        var worker = new VisitedAreasWorker(
            NullLogger<VisitedAreasWorker>.Instance,
            activities.Object,
            new Mock<IActivityVisitedAreaRepository>().Object,
            protectedAreas.Object,
            adminBoundaries.Object,
            syncStatus,
            ingestion.Object);

        await worker.ExecuteAsync(new ActivityIdJob { ActivityId = activityId }, CancellationToken.None);

        var protectedIngestIndex = callOrder.IndexOf("ingestProtected");
        var protectedQueryIndex = callOrder.IndexOf("queryProtected");
        Assert.True(protectedIngestIndex >= 0 && protectedIngestIndex < protectedQueryIndex);

        var adminIngestIndex = callOrder.IndexOf("ingestAdmin");
        var adminQueryIndex = callOrder.IndexOf("queryAdmin");
        Assert.True(adminIngestIndex >= 0 && adminIngestIndex < adminQueryIndex);

        ingestion.Verify(
            s => s.EnsureProtectedAreasAsync(It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<CancellationToken>()),
            Times.Once);
        ingestion.Verify(
            s => s.EnsureAdminBoundariesAsync(
                It.IsAny<IEnumerable<Coordinate>>(), 4, It.IsAny<CancellationToken>()),
            Times.Once);
    }

    private static Mock<IOsmFeatureIngestionService> CreateIngestionMock()
    {
        var mock = new Mock<IOsmFeatureIngestionService>();
        mock.Setup(s => s.EnsureProtectedAreasAsync(It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OsmIngestionResult.Empty);
        mock.Setup(s => s.EnsureAdminBoundariesAsync(
                It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OsmIngestionResult.Empty);
        return mock;
    }

    private static StoredFeatureSummary CreateSummary(string id, string name)
        => new()
        {
            Id = id,
            FeatureId = id,
            Kind = "adminBoundary",
            Properties = new Dictionary<string, dynamic>
            {
                ["name"] = name
            }
        };

    private static StoredFeature CreateBoundary(string id, Geometry geometry)
        => new()
        {
            Id = id,
            FeatureId = id,
            Kind = "adminBoundary",
            X = 0,
            Y = 0,
            Zoom = 6,
            Geometry = geometry,
            Centroid = new Coordinate(0, 0),
            Properties = new Dictionary<string, dynamic>()
        };

    private static Polygon CreateSquare(double minLng, double minLat, double maxLng, double maxLat)
        => new(
            [
                new LinearRing(
                    [
                        new Position(minLng, minLat),
                        new Position(maxLng, minLat),
                        new Position(maxLng, maxLat),
                        new Position(minLng, maxLat),
                        new Position(minLng, minLat)
                    ],
                    null)
            ],
            null);
}
