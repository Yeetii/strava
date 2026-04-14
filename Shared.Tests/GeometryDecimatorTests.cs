using BAMCIS.GeoJSON;
using Shared.Geo;

namespace Shared.Tests;

public class GeometryDecimatorTests
{
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