using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class StoredFeatureSummaryTests
{
    // ── ResolvedCentroid ────────────────────────────────────────────────────

    [Fact]
    public void ResolvedCentroid_WhenCentroidSet_ReturnsThatCentroid()
    {
        var summary = new StoredFeatureSummary
        {
            Id = "peak:1",
            Kind = FeatureKinds.Peak,
            Centroid = new Coordinate(13.5, 63.5)
        };

        var centroid = summary.ResolvedCentroid;

        Assert.Equal(13.5, centroid.Lng);
        Assert.Equal(63.5, centroid.Lat);
    }

    [Fact]
    public void ResolvedCentroid_WhenCentroidNull_ThrowsInvalidOperationException()
    {
        var summary = new StoredFeatureSummary
        {
            Id = "peak:no-centroid",
            Kind = FeatureKinds.Peak,
            Centroid = null
        };

        Assert.Throws<InvalidOperationException>(() => summary.ResolvedCentroid);
    }

    // ── IsPointer ───────────────────────────────────────────────────────────

    [Fact]
    public void IsPointer_WhenPointerFlagTrue_ReturnsTrue()
    {
        var summary = new StoredFeatureSummary
        {
            Id = "peak:ptr",
            Properties = new Dictionary<string, dynamic>
            {
                [StoredFeature.PointerFlagProperty] = true
            }
        };

        Assert.True(summary.IsPointer);
    }

    [Fact]
    public void IsPointer_WhenPointerFlagAbsent_ReturnsFalse()
    {
        var summary = new StoredFeatureSummary { Id = "peak:1" };
        Assert.False(summary.IsPointer);
    }

    [Fact]
    public void IsPointer_WhenPointerFlagFalse_ReturnsFalse()
    {
        var summary = new StoredFeatureSummary
        {
            Id = "peak:1",
            Properties = new Dictionary<string, dynamic>
            {
                [StoredFeature.PointerFlagProperty] = false
            }
        };

        Assert.False(summary.IsPointer);
    }

    // ── StoredDocumentId ────────────────────────────────────────────────────

    [Fact]
    public void StoredDocumentId_WhenPropertyPresent_ReturnsValue()
    {
        var summary = new StoredFeatureSummary
        {
            Id = "peak:ptr",
            Properties = new Dictionary<string, dynamic>
            {
                [StoredFeature.PointerStoredDocumentIdProperty] = "peak:way:123"
            }
        };

        Assert.Equal("peak:way:123", summary.StoredDocumentId);
    }

    [Fact]
    public void StoredDocumentId_WhenPropertyAbsent_ReturnsNull()
    {
        var summary = new StoredFeatureSummary { Id = "peak:1" };
        Assert.Null(summary.StoredDocumentId);
    }

    // ── FromStoredFeature ───────────────────────────────────────────────────

    [Fact]
    public void FromStoredFeature_CopiesAllScalarFields()
    {
        var feature = new StoredFeature
        {
            Id = "peak:42",
            FeatureId = "42",
            Kind = FeatureKinds.Peak,
            X = 10,
            Y = 20,
            Zoom = 11,
            Centroid = new Coordinate(13.5, 63.5),
            Geometry = new Point(new Position(13.5, 63.5)),
            Properties = new Dictionary<string, dynamic> { ["ele"] = "1200" }
        };

        var summary = StoredFeatureSummary.FromStoredFeature(feature);

        Assert.Equal("peak:42", summary.Id);
        Assert.Equal("42", summary.FeatureId);
        Assert.Equal(FeatureKinds.Peak, summary.Kind);
        Assert.Equal(10, summary.X);
        Assert.Equal(20, summary.Y);
        Assert.Equal(11, summary.Zoom);
        Assert.Equal(13.5, summary.Centroid!.Lng);
        Assert.Equal(63.5, summary.Centroid!.Lat);
    }

    [Fact]
    public void FromStoredFeature_UsesCentroidWhenStored()
    {
        var feature = new StoredFeature
        {
            Id = "peak:1",
            Kind = FeatureKinds.Peak,
            X = 0, Y = 0, Zoom = 11,
            Centroid = new Coordinate(5.0, 10.0),
            Geometry = new Point(new Position(6.0, 11.0)) // different point — centroid wins
        };

        var summary = StoredFeatureSummary.FromStoredFeature(feature);

        Assert.Equal(5.0, summary.Centroid!.Lng);
        Assert.Equal(10.0, summary.Centroid!.Lat);
    }

    [Fact]
    public void FromStoredFeature_FallsBackToGeometryCentroidWhenCentroidNotStored()
    {
        var feature = new StoredFeature
        {
            Id = "peak:1",
            Kind = FeatureKinds.Peak,
            X = 0, Y = 0, Zoom = 11,
            Centroid = null,
            Geometry = new Point(new Position(13.5, 63.5)) // lng, lat
        };

        var summary = StoredFeatureSummary.FromStoredFeature(feature);

        Assert.NotNull(summary.Centroid);
        Assert.Equal(13.5, summary.Centroid.Lng, precision: 4);
        Assert.Equal(63.5, summary.Centroid.Lat, precision: 4);
    }

    [Fact]
    public void FromStoredFeature_NullGeometryAndNullCentroidYieldsNullCentroid()
    {
        var feature = new StoredFeature
        {
            Id = "peak:1",
            Kind = FeatureKinds.Peak,
            X = 0, Y = 0, Zoom = 11,
            Centroid = null,
            // StoredFeature.Geometry is required, so use a minimal placeholder
            Geometry = null!
        };

        var summary = StoredFeatureSummary.FromStoredFeature(feature);

        Assert.Null(summary.Centroid);
    }
}
