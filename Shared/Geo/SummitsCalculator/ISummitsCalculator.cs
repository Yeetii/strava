using Shared.Models;

namespace Shared.Geo.SummitsCalculator;

public interface ISummitsCalculator
{
    IEnumerable<T> FindPointsNearRoute<T>(IEnumerable<(T, Coordinate)> points, string polylineString, int maxIntersectDistance = 50);
}