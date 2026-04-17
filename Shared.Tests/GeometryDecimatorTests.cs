using BAMCIS.GeoJSON;
using Shared.Geo;

namespace Shared.Tests;

public class GeometryDecimatorTests
{
    // --- RDP Simplify tests ---------------------------------------------------

    [Fact]
    public void Simplify_CollinearPointsAreRemoved()
    {
        // A ring whose edges are straight lines — all intermediate collinear
        // points should be removed by RDP with a small epsilon.
        var polygon = new Polygon(
        [
            new LinearRing(
            [
                new Position(0, 0),
                new Position(1, 0),
                new Position(2, 0),   // collinear
                new Position(3, 0),   // collinear
                new Position(4, 0),
                new Position(4, 4),
                new Position(0, 4),
                new Position(0, 0)
            ], null)
        ], null);

        var simplified = Assert.IsType<Polygon>(GeometryDecimator.Simplify(polygon, 0.001));
        var coords = simplified.Coordinates.Single().Coordinates.ToList();

        // The two collinear points (2,0) and (3,0) should be gone.
        Assert.True(coords.Count < 8, $"Expected fewer points, got {coords.Count}");
        // Ring must still be closed.
        Assert.Equal(coords[0].Longitude, coords[^1].Longitude);
        Assert.Equal(coords[0].Latitude, coords[^1].Latitude);
        Assert.True(coords.Count >= 4, "Ring needs at least 4 positions (3 + close)");
    }

    [Fact]
    public void Simplify_SharpCornersArePreserved()
    {
        // A zig-zag with sharp deviations — even with a moderate epsilon,
        // the corner points should survive.
        var polygon = new Polygon(
        [
            new LinearRing(
            [
                new Position(0, 0),
                new Position(5, 0),
                new Position(5, 5),
                new Position(0, 5),
                new Position(0, 0)
            ], null)
        ], null);

        // Epsilon is smaller than the side lengths, so every corner should survive.
        var simplified = Assert.IsType<Polygon>(GeometryDecimator.Simplify(polygon, 0.5));
        var coords = simplified.Coordinates.Single().Coordinates.ToList();

        Assert.Equal(5, coords.Count); // 4 corners + closing point
    }

    [Fact]
    public void Simplify_MultiPolygonPreservesBothPolygons()
    {
        var multiPolygon = new MultiPolygon(
        [
            CreateSquarePolygon(0, 0, 4, 4),
            CreateSquarePolygon(10, 10, 14, 14)
        ], null);

        var simplified = Assert.IsType<MultiPolygon>(GeometryDecimator.Simplify(multiPolygon, 0.001));
        Assert.Equal(2, simplified.Coordinates.Count());
    }

    [Fact]
    public void Simplify_ZeroEpsilonReturnsOriginal()
    {
        var polygon = CreateSquarePolygon(0, 0, 4, 4);
        var result = GeometryDecimator.Simplify(polygon, 0);
        Assert.Same(polygon, result);
    }

    // --- Legacy Decimate tests ------------------------------------------------

    [Fact]
    public void DecimatePolygon_DropsIntermediateCoordinatesButKeepsRingClosed()
    {
        var polygon = new Polygon(
        [
            new LinearRing(
            [
                new Position(0, 0),
                new Position(1, 0),
                new Position(2, 0),
                new Position(3, 0),
                new Position(3, 1),
                new Position(2, 1),
                new Position(1, 1),
                new Position(0, 1),
                new Position(0, 0)
            ], null)
        ], null);

        var decimated = Assert.IsType<Polygon>(GeometryDecimator.Decimate(polygon, 2));
        var coordinates = decimated.Coordinates.Single().Coordinates.ToList();

        Assert.True(coordinates.Count < polygon.Coordinates.Single().Coordinates.Count());
        Assert.Equal(coordinates[0].Longitude, coordinates[^1].Longitude);
        Assert.Equal(coordinates[0].Latitude, coordinates[^1].Latitude);
        Assert.True(coordinates.Count >= 4);
    }

    [Fact]
    public void DecimateMultiPolygon_PreservesMultiplePolygons()
    {
        var multiPolygon = new MultiPolygon(
        [
            CreateSquarePolygon(0, 0, 4, 4),
            CreateSquarePolygon(10, 10, 14, 14)
        ], null);

        var decimated = Assert.IsType<MultiPolygon>(GeometryDecimator.Decimate(multiPolygon, 2));

        Assert.Equal(2, decimated.Coordinates.Count());
    }

    private static Polygon CreateSquarePolygon(double minX, double minY, double maxX, double maxY)
    {
        return new Polygon(
        [
            new LinearRing(
            [
                new Position(minX, minY),
                new Position((minX + maxX) / 2, minY),
                new Position(maxX, minY),
                new Position(maxX, (minY + maxY) / 2),
                new Position(maxX, maxY),
                new Position((minX + maxX) / 2, maxY),
                new Position(minX, maxY),
                new Position(minX, (minY + maxY) / 2),
                new Position(minX, minY)
            ], null)
        ], null);
    }
}