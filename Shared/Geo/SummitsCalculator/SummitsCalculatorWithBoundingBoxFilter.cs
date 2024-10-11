using Shared.Models;

namespace Shared.Geo.SummitsCalculator;

public class SummitsCalculatorWithBoundingBoxFilter() : ISummitsCalculator
{
    private readonly BasicSummitsCalculator _basicSummitsCalculator = new();

    public IEnumerable<T> FindPointsNearRoute<T>(IEnumerable<(T, Coordinate)> points, string polylineString, int maxIntersectDistance = 50)
    {
        var polyline = GeoSpatialFunctions.DecodePolyline(polylineString);

        var max = new Coordinate(double.MinValue, double.MinValue);
        var min = new Coordinate(double.MaxValue, double.MaxValue);

        foreach (var point in polyline)
        {
            if (point.Lat < min.Lat)
                min.Lat = point.Lat;
            if (point.Lat > max.Lat)
                max.Lat = point.Lat;
            if (point.Lng < min.Lng)
                min.Lng = point.Lng;
            if (point.Lng > max.Lng)
                max.Lng = point.Lng;
        }

        max = GeoSpatialFunctions.ShiftCoordinate(max, maxIntersectDistance + 10, maxIntersectDistance + 10);
        min = GeoSpatialFunctions.ShiftCoordinate(min, -maxIntersectDistance - 10, -maxIntersectDistance - 10);

        var filteredPoints = points.Where(p =>
        {
            var lat = p.Item2.Lat;
            var lng = p.Item2.Lng;
            return lat >= min.Lat && lat <= max.Lat && lng >= min.Lng && lng <= max.Lng;
        });
        return _basicSummitsCalculator.FindPointsNearRoute(filteredPoints, polyline, maxIntersectDistance);
    }
}