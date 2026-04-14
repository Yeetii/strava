using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class StoredFeatureTests
{
    [Fact]
    public void LogicalId_UsesFeatureIdWhenPresent()
    {
        var feature = new StoredFeature
        {
            Id = "peak:11846352799",
            FeatureId = "11846352799",
            Kind = FeatureKinds.Peak,
            X = 0,
            Y = 0,
            Geometry = new Point(new Position(0, 0))
        };

        Assert.Equal("11846352799", feature.LogicalId);
    }

    [Fact]
    public void LogicalId_StripsKindPrefixWhenFeatureIdIsMissing()
    {
        var feature = new StoredFeature
        {
            Id = "peak:11846352799",
            Kind = FeatureKinds.Peak,
            X = 0,
            Y = 0,
            Geometry = new Point(new Position(0, 0))
        };

        Assert.Equal("11846352799", feature.LogicalId);
    }

    [Fact]
    public void EnsurePrefixedFeatureId_DoesNotDoublePrefix()
    {
        var prefixedId = StoredFeature.EnsurePrefixedFeatureId(FeatureKinds.Peak, "peak:11846352799");

        Assert.Equal("peak:11846352799", prefixedId);
    }

    [Fact]
    public void CreatePointer_PopulatesStoredLocationMetadata()
    {
        var pointer = StoredFeature.CreatePointer(
            FeatureKinds.Path,
            "way:123",
            x: 10,
            y: 20,
            zoom: 11,
            storedX: 12,
            storedY: 21,
            storedZoom: 11,
            storedDocumentId: "path:way:123");

        Assert.True(StoredFeature.IsPointerDocument(pointer));
        Assert.Equal("path:way:123", StoredFeature.GetPointerStoredDocumentId(pointer));
        Assert.Equal("way:123", pointer.Properties[StoredFeature.PointerFeatureIdProperty]);
        Assert.Equal(12, pointer.Properties[StoredFeature.PointerStoredXProperty]);
        Assert.Equal(21, pointer.Properties[StoredFeature.PointerStoredYProperty]);
        Assert.Equal(11, pointer.Properties[StoredFeature.PointerStoredZoomProperty]);
    }
}