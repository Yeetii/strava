using Shared.Models;

namespace Shared.Geo.SummitsCalculator;

public class SummitsCalculatorWithSimpleFilter(int activityLength) : ISummitsCalculator
{
    private readonly BasicSummitsCalculator _basicSummitsCalculator = new();

    public IEnumerable<T> FindPointsNearRoute<T>(IEnumerable<(T, Coordinate)> points, string polylineString, int maxIntersectDistance = 50)
    {
        var polyline = GeoSpatialFunctions.DecodePolyline(polylineString);
        var startLocation = polyline.First();
        var filteredPeaks = points.Where(peak => GeoSpatialFunctions.DistanceTo(peak.Item2, startLocation) < activityLength).ToList();

        return _basicSummitsCalculator.FindPointsNearRoute(filteredPeaks, polyline, maxIntersectDistance);
    }
}