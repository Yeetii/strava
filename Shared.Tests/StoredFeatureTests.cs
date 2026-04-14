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
}