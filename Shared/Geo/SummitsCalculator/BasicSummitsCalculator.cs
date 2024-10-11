using Shared.Models;

namespace Shared.Geo.SummitsCalculator;

public class BasicSummitsCalculator : ISummitsCalculator
{
    public IEnumerable<T> FindPointsNearRoute<T>(IEnumerable<(T, Coordinate)> points, string polylineString, int maxIntersectDistance = 50)
    {
        IEnumerable<Coordinate> polyline = GeoSpatialFunctions.DecodePolyline(polylineString);
        return FindPointsNearRoute(points, polyline, maxIntersectDistance);
    }

    public IEnumerable<T> FindPointsNearRoute<T>(IEnumerable<(T, Coordinate)> points, IEnumerable<Coordinate> polyline, int maxIntersectDistance = 50)
    {
        foreach (var (obj, location) in points)
        {
            foreach (Coordinate coordinate in polyline)
            {
                var distance = GeoSpatialFunctions.DistanceTo(location, coordinate);
                if (distance <= maxIntersectDistance)
                {
                    yield return obj;
                    break;
                }
            }
        }
    }
}