using Stigar.Core.Jobs;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Stigar.Core.Constants;
using Stigar.Core.Geo.SummitsCalculator;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.Postgres.Spatial;
using Stigar.Core.Services.Postgres.Spatial.Ingestion;

namespace Stigar.Workers.Tests;

public class SummitsWorkerTests
{
    [Fact]
    public void BuildSummitedPeakDocumentId_UsesFeatureIdValueInsteadOfTypeName()
    {
        var documentId = SummitsWorker.BuildSummitedPeakDocumentId("122316632", new FeatureId("11908635"));

        Assert.Equal("122316632-11908635", documentId);
        Assert.DoesNotContain("BAMCIS.GeoJSON.FeatureId", documentId, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildSummitedPeakDocumentId_NormalizesPrefixedPeakIds()
    {
        var documentId = SummitsWorker.BuildSummitedPeakDocumentId("user-1", new FeatureId("peak:11908635"));

        Assert.Equal("user-1-11908635", documentId);
    }

    [Fact]
    public void ToStoredPeakId_UsesTheIdFormatStoredInThePeaksTable()
    {
        Assert.Equal("peak:11908635", PeakIdRules.ToStoredPeakId(new FeatureId("11908635")));
        Assert.Equal("peak:11908635", PeakIdRules.ToStoredPeakId(new FeatureId("peak:11908635")));
    }

    [Fact]
    public async Task Run_WritesSummitLinksUsingPostgresRepositories()
    {
        const string userId = "user-1";
        const string activityId = "activity-1";
        const string peakId = "11908635";
        // Ingestion stores peaks with a kind-prefixed Id; the FK on activity_summits.peak_id
        // requires the summit link to use that exact stored id, not the normalized feature id.
        const string storedPeakId = "peak:11908635";

        var activity = CreateActivity(activityId, userId, hasGeodata: true);
        var peak = new StoredFeature
        {
            Id = storedPeakId,
            FeatureId = peakId,
            Kind = FeatureKinds.Peak,
            X = 0,
            Y = 0,
            Zoom = 11,
            Geometry = new Point(new Position(-120.2, 38.5)),
            Properties = new Dictionary<string, dynamic>
            {
                ["name"] = "Test Peak",
                ["ele"] = "1000"
            }
        };

        var activities = new Mock<IActivityRepository>();
        activities
            .Setup(r => r.GetByIds(It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([activity]);
        activities
            .Setup(r => r.GetById(activityId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);

        var peaks = new Mock<IPeakRepository>();
        peaks
            .Setup(r => r.GetByTiles(It.IsAny<IEnumerable<(int, int)>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([peak]);

        var calculator = new Mock<ISummitsCalculator>();
        calculator
            .Setup(c => c.FindPointsNearRoute(
                It.IsAny<IEnumerable<(string, Coordinate)>>(),
                It.IsAny<string>(),
                It.IsAny<int>()))
            .Returns((IEnumerable<(string, Coordinate)> points, string _, int __) => points.Select(p => p.Item1).ToList());

        var summitLinks = new Mock<IActivitySummitRepository>();
        var users = new Mock<IUserRepository>();
        var syncStatus = new UserSyncStatusService(users.Object, activities.Object);

        var worker = new SummitsWorker(
            NullLogger<SummitsWorker>.Instance,
            activities.Object,
            summitLinks.Object,
            peaks.Object,
            calculator.Object,
            syncStatus,
            CreateIngestionMock().Object);

        await worker.ExecuteAsync(new ActivityIdJob { ActivityId = activityId }, CancellationToken.None);

        peaks.Verify(
            r => r.GetByTiles(It.IsAny<IEnumerable<(int, int)>>(), It.IsAny<CancellationToken>()),
            Times.Once);
        summitLinks.Verify(r => r.RemoveByActivity(activityId, It.IsAny<CancellationToken>()), Times.Once);
        summitLinks.Verify(
            r => r.AddRange(
                It.Is<IEnumerable<SummitLink>>(links => links.Count() == 1
                    && links.First().ActivityId == activityId
                    && links.First().UserId == userId
                    && links.First().PeakId == storedPeakId),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Run_SkipsActivityWithoutGeodata()
    {
        const string activityId = "activity-1";
        var activity = CreateActivity(activityId, "user-1", hasGeodata: false);

        var activities = new Mock<IActivityRepository>();
        activities
            .Setup(r => r.GetByIds(It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([activity]);
        activities
            .Setup(r => r.GetById(activityId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);

        var peaks = new Mock<IPeakRepository>();
        var summitLinks = new Mock<IActivitySummitRepository>();
        var users = new Mock<IUserRepository>();
        var syncStatus = new UserSyncStatusService(users.Object, activities.Object);

        var worker = new SummitsWorker(
            NullLogger<SummitsWorker>.Instance,
            activities.Object,
            summitLinks.Object,
            peaks.Object,
            Mock.Of<ISummitsCalculator>(),
            syncStatus,
            CreateIngestionMock().Object);

        await worker.ExecuteAsync(new ActivityIdJob { ActivityId = activityId }, CancellationToken.None);
        peaks.Verify(
            r => r.GetByTiles(It.IsAny<IEnumerable<(int, int)>>(), It.IsAny<CancellationToken>()),
            Times.Never);
        summitLinks.Verify(r => r.RemoveByActivity(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        summitLinks.Verify(r => r.AddRange(It.IsAny<IEnumerable<SummitLink>>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Run_EnsuresPeaksBeforeSpatialLookup()
    {
        const string activityId = "activity-1";
        var activity = CreateActivity(activityId, "user-1", hasGeodata: true);
        var peak = new StoredFeature
        {
            Id = "11908635",
            FeatureId = "11908635",
            Kind = FeatureKinds.Peak,
            X = 0,
            Y = 0,
            Zoom = 11,
            Geometry = new Point(new Position(-120.2, 38.5)),
            Properties = new Dictionary<string, dynamic> { ["name"] = "Test Peak" }
        };

        var callOrder = new List<string>();
        IEnumerable<Coordinate>? ingestedPoints = null;

        var activities = new Mock<IActivityRepository>();
        activities
            .Setup(r => r.GetByIds(It.IsAny<IEnumerable<string>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([activity]);
        activities
            .Setup(r => r.GetById(activityId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(activity);

        var ingestion = CreateIngestionMock();
        ingestion
            .Setup(s => s.EnsurePeaksAsync(It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<CancellationToken>()))
            .Callback<IEnumerable<Coordinate>, CancellationToken>((points, _) =>
            {
                ingestedPoints = points;
                callOrder.Add("ingest");
            })
            .ReturnsAsync(new OsmIngestionResult(1, 1, 1, 0));

        var peaks = new Mock<IPeakRepository>();
        peaks
            .Setup(r => r.GetByTiles(It.IsAny<IEnumerable<(int, int)>>(), It.IsAny<CancellationToken>()))
            .Callback(() => callOrder.Add("query"))
            .ReturnsAsync([peak]);

        var calculator = new Mock<ISummitsCalculator>();
        calculator
            .Setup(c => c.FindPointsNearRoute(
                It.IsAny<IEnumerable<(string, Coordinate)>>(),
                It.IsAny<string>(),
                It.IsAny<int>()))
            .Returns((IEnumerable<(string, Coordinate)> points, string _, int __) => points.Select(p => p.Item1).ToList());

        var users = new Mock<IUserRepository>();
        var syncStatus = new UserSyncStatusService(users.Object, activities.Object);
        var worker = new SummitsWorker(
            NullLogger<SummitsWorker>.Instance,
            activities.Object,
            new Mock<IActivitySummitRepository>().Object,
            peaks.Object,
            calculator.Object,
            syncStatus,
            ingestion.Object);

        await worker.ExecuteAsync(new ActivityIdJob { ActivityId = activityId }, CancellationToken.None);

        Assert.NotEmpty(callOrder);
        Assert.Equal("ingest", callOrder[0]);
        Assert.Contains("query", callOrder);
        Assert.NotNull(ingestedPoints);
        Assert.NotEmpty(ingestedPoints);
        ingestion.Verify(
            s => s.EnsurePeaksAsync(It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    private static Mock<IOsmFeatureIngestionService> CreateIngestionMock()
    {
        var mock = new Mock<IOsmFeatureIngestionService>();
        mock.Setup(s => s.EnsurePeaksAsync(It.IsAny<IEnumerable<Coordinate>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(OsmIngestionResult.Empty);
        return mock;
    }

    private static Activity CreateActivity(string id, string userId, bool hasGeodata) => new()
    {
        Id = id,
        UserId = userId,
        Name = "Morning Run",
        SportType = SportTypes.RUN,
        StartDate = DateTime.UtcNow,
        StartDateLocal = DateTime.UtcNow,
        StartLatLng = hasGeodata ? new float[] { 38.5f, -120.2f } : null,
        SummaryPolyline = hasGeodata ? "_p~iF~ps|U_ulLnnqC_mqNvxq`@`" : null,
        ProcessingStatus = new ActivityProcessingStatus { SummitedPeaksEpoch = 1 }
    };
}
