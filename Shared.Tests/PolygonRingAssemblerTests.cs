using BAMCIS.GeoJSON;
using Shared.Geo;

namespace Shared.Tests;

public class PolygonRingAssemblerTests
{
    [Fact]
    public void SingleClosedSegment_ReturnsOneRing()
    {
        // A single way that is already a closed ring
        var segment = new List<Position>
        {
            new(0, 0), new(1, 0), new(1, 1), new(0, 1), new(0, 0)
        };

        var rings = PolygonRingAssembler.AssembleRings([segment]).ToList();

        Assert.Single(rings);
        Assert.Equal(5, rings[0].Coordinates.Count());
    }

    [Fact]
    public void TwoSegmentsFormingOneRing_ReturnsOneRing()
    {
        // A boundary split into two open ways: A→B and B→A
        var seg1 = new List<Position> { new(0, 0), new(1, 0), new(1, 1) };
        var seg2 = new List<Position> { new(1, 1), new(0, 1), new(0, 0) };

        var rings = PolygonRingAssembler.AssembleRings([seg1, seg2]).ToList();

        Assert.Single(rings);
        // Assembled ring: (0,0)→(1,0)→(1,1)→(0,1)→(0,0) = 5 positions
        Assert.Equal(5, rings[0].Coordinates.Count());
    }

    [Fact]
    public void FourSegmentsFormingOneRing_ReturnsOneRing()
    {
        // A square boundary split into 4 sides
        var seg1 = new List<Position> { new(0, 0), new(1, 0) };
        var seg2 = new List<Position> { new(1, 0), new(1, 1) };
        var seg3 = new List<Position> { new(1, 1), new(0, 1) };
        var seg4 = new List<Position> { new(0, 1), new(0, 0) };

        var rings = PolygonRingAssembler.AssembleRings([seg1, seg2, seg3, seg4]).ToList();

        Assert.Single(rings);
        Assert.Equal(5, rings[0].Coordinates.Count());
    }

    [Fact]
    public void SegmentsInWrongOrder_AreAssembledCorrectly()
    {
        // Segments provided out of order; assembler should still connect them
        var seg1 = new List<Position> { new(0, 0), new(1, 0) };
        var seg3 = new List<Position> { new(1, 1), new(0, 1) };
        var seg2 = new List<Position> { new(1, 0), new(1, 1) };
        var seg4 = new List<Position> { new(0, 1), new(0, 0) };

        var rings = PolygonRingAssembler.AssembleRings([seg1, seg3, seg2, seg4]).ToList();

        Assert.Single(rings);
        Assert.Equal(5, rings[0].Coordinates.Count());
    }

    [Fact]
    public void SegmentNeedingReversal_IsHandled()
    {
        // seg2 is stored in reverse order; assembler should reverse it to connect
        var seg1 = new List<Position> { new(0, 0), new(1, 0), new(1, 1) };
        var seg2 = new List<Position> { new(0, 0), new(0, 1), new(1, 1) }; // reversed: should connect (1,1)→(0,1)→(0,0)

        var rings = PolygonRingAssembler.AssembleRings([seg1, seg2]).ToList();

        Assert.Single(rings);
        Assert.Equal(5, rings[0].Coordinates.Count());
    }

    [Fact]
    public void TwoDisjointRings_ReturnsTwoRings()
    {
        // Two completely separate closed ways (e.g. a multipolygon with two outer rings)
        var ring1seg1 = new List<Position> { new(0, 0), new(1, 0) };
        var ring1seg2 = new List<Position> { new(1, 0), new(1, 1) };
        var ring1seg3 = new List<Position> { new(1, 1), new(0, 0) };

        var ring2seg1 = new List<Position> { new(10, 10), new(11, 10) };
        var ring2seg2 = new List<Position> { new(11, 10), new(11, 11) };
        var ring2seg3 = new List<Position> { new(11, 11), new(10, 10) };

        var rings = PolygonRingAssembler.AssembleRings(
            [ring1seg1, ring1seg2, ring1seg3, ring2seg1, ring2seg2, ring2seg3]).ToList();

        Assert.Equal(2, rings.Count);
    }

    [Fact]
    public void PointInPolygon_WorksForAssembledRingGeometry()
    {
        // Simulate a large protected area whose boundary is split into 4 way segments.
        // The assembled polygon should correctly report a center point as inside.
        var seg1 = new List<Position> { new(0, 0), new(2, 0) };
        var seg2 = new List<Position> { new(2, 0), new(2, 2) };
        var seg3 = new List<Position> { new(2, 2), new(0, 2) };
        var seg4 = new List<Position> { new(0, 2), new(0, 0) };

        var ring = PolygonRingAssembler.AssembleRings([seg1, seg2, seg3, seg4]).Single();
        var polygon = new Polygon([ring], null);

        var insidePoint = new Shared.Models.Coordinate(1, 1);
        var outsidePoint = new Shared.Models.Coordinate(3, 3);

        Assert.True(RouteFeatureMatcher.IsPointInGeometry(insidePoint, polygon));
        Assert.False(RouteFeatureMatcher.IsPointInGeometry(outsidePoint, polygon));
    }
}
