using Shared.Geo;
using Shared.Models;

namespace Shared.Tests;

public class GeoSpatialFunctionsTests
{

    [Fact]
    public void DecodePolyLineTest()
    {
        string polyline = "o_}aKcq|nAAASZU`@Yt@OVu@nBSRIo@Me@";
        var line = GeoSpatialFunctions.DecodePolyline(polyline).ToArray();

        List<Coordinate> coordinates =
        [
            new Coordinate(13.09474, 63.39592),
            new Coordinate(13.09475, 63.39593),
            new Coordinate(13.09461, 63.39603),
            new Coordinate(13.09444, 63.39614),
            new Coordinate(13.09417, 63.39627),
            new Coordinate(13.09405, 63.39635),
            new Coordinate(13.09349, 63.39662),
            new Coordinate(13.09339, 63.39672),
            new Coordinate(13.09363, 63.39677),
            new Coordinate(13.09382, 63.39684)
        ];

        for (int i = 0; i < line.Length; i++)
        {
            Assert.Equal(
            coordinates[i].Lat,
            line[i].Lat,
            5
            );
            Assert.Equal(
            coordinates[i].Lng,
            line[i].Lng,
            5
            );
        }
    }

    [Fact]
    public void ShiftCoordinateTest()
    {
        Coordinate point1 = new(63.39677, 13.09363);
        var movedPoint = GeoSpatialFunctions.ShiftCoordinate(point1, 50, 0);
        var distanceMoved = GeoSpatialFunctions.DistanceTo(point1, movedPoint);
        Assert.Equal(50, distanceMoved, 1);
    }

    [Fact]
    public void DistanceToTest()
    {
        Coordinate point1 = new(63.39677, 13.09363);
        Coordinate point2 = new(63.39677, 13.09363);
        Coordinate point3 = new(63.40841, 13.11212);
        double distance = GeoSpatialFunctions.DistanceTo(point1, point2);
        // Should be 2,4km according to https://www.calculator.net/distance-calculator.html?type=3&la1=13.09363&lo1=63.39677&la2=13.11212&lo2=63.40841&ctype=dec&lad1=38&lam1=53&las1=51.36&lau1=n&lod1=77&lom1=2&los1=11.76&lou1=w&lad2=39&lam2=56&las2=58.56&lau2=n&lod2=75&lom2=9&los2=1.08&lou2=w&x=91&y=12#latlog
        double distance2 = GeoSpatialFunctions.DistanceTo(point1, point3);
        Assert.Equal(0, distance);
        Assert.True(distance2 > 2350);
        Assert.True(distance2 < 2450);
    }

    [Fact]
    public void DistanceToMustWorkForSmallDistances()
    {
        Coordinate point1 = new(63.40324, 13.08618);
        Coordinate point2 = new(63.40324, 13.086179);
        double distance = GeoSpatialFunctions.DistanceTo(point1, point2);
        // Reference https://www.omnicalculator.com/other/latitude-longitude-distance
        Assert.Equal(0.1112, distance, 4);
    }

    public static IEnumerable<object[]> MaxDistanceData =>
        [
            [new List<Coordinate> { new(14, 13), new(14.001, 13) }, 108.35],
            [new List<Coordinate> { new(13.094740000000002, 63.395920000000004), new(13.094750000000001, 63.39593000000001), new(13.094750000000001, 63.39593000000001) }, 1.22],
            [new List<Coordinate> { new(1, 1), new(1, 1) }, 0],
        ];

    [Theory]
    [MemberData(nameof(MaxDistanceData))]
    public void MaxDistanceShouldCalculateMaxDistanceBetweenTwoPoints(IEnumerable<Coordinate> points, double expected)
    {
        var result = GeoSpatialFunctions.MaxDistance(points);
        Assert.Equal(expected, result, 2);
    }
}
