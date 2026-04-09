using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Geo;

public static class RouteFeatureMatcher
{
    public static bool RouteIntersectsLine(string polylineString, IEnumerable<Coordinate> line, int maxIntersectDistance = 50)
    {
        var routeCoordinates = GeoSpatialFunctions.DecodePolyline(polylineString).ToList();
        var lineCoordinates = line.ToList();
        if (routeCoordinates.Count == 0 || lineCoordinates.Count == 0)
        {
            return false;
        }

        if (!BoundsOverlap(GetBounds(routeCoordinates, maxIntersectDistance), GetBounds(lineCoordinates, maxIntersectDistance)))
        {
            return false;
        }

        foreach (var routePoint in routeCoordinates)
        {
            foreach (var linePoint in lineCoordinates)
            {
                if (GeoSpatialFunctions.DistanceTo(routePoint, linePoint) <= maxIntersectDistance)
                {
                    return true;
                }
            }
        }

        return false;
    }

    public static bool IsPointInGeometry(Coordinate point, Geometry geometry)
    {
        foreach (var ring in GetOuterRings(geometry))
        {
            var bounds = GetBounds(ring, 0);
            if (Contains(bounds, point) && PointInPolygon(point, ring))
            {
                return true;
            }
        }
        return false;
    }

    public static bool RouteIntersectsPolygon(string polylineString, Geometry geometry)
    {
        var routeCoordinates = GeoSpatialFunctions.DecodePolyline(polylineString).ToList();
        if (routeCoordinates.Count == 0)
        {
            return false;
        }

        foreach (var ring in GetOuterRings(geometry))
        {
            var bounds = GetBounds(ring, 0);
            foreach (var routePoint in routeCoordinates)
            {
                if (Contains(bounds, routePoint) && PointInPolygon(routePoint, ring))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static IEnumerable<List<Coordinate>> GetOuterRings(Geometry geometry)
    {
        switch (geometry)
        {
            case Polygon polygon:
                {
                    var outerRing = polygon.Coordinates.FirstOrDefault();
                    if (outerRing != null)
                    {
                        yield return outerRing.Coordinates.Select(ToCoordinate).ToList();
                    }
                    break;
                }
            case MultiPolygon multiPolygon:
                {
                    foreach (var polygon in multiPolygon.Coordinates)
                    {
                        var outerRing = polygon.Coordinates.FirstOrDefault();
                        if (outerRing != null)
                        {
                            yield return outerRing.Coordinates.Select(ToCoordinate).ToList();
                        }
                    }
                    break;
                }
        }
    }

    private static Coordinate ToCoordinate(Position position)
    {
        return new Coordinate(position.Longitude, position.Latitude);
    }

    private static (Coordinate min, Coordinate max) GetBounds(IEnumerable<Coordinate> coordinates, int paddingMeters)
    {
        var coordinateList = coordinates.ToList();
        var min = new Coordinate(
            coordinateList.Min(coordinate => coordinate.Lng),
            coordinateList.Min(coordinate => coordinate.Lat)
        );
        var max = new Coordinate(
            coordinateList.Max(coordinate => coordinate.Lng),
            coordinateList.Max(coordinate => coordinate.Lat)
        );

        if (paddingMeters == 0)
        {
            return (min, max);
        }

        return (
            GeoSpatialFunctions.ShiftCoordinate(min, -paddingMeters, -paddingMeters),
            GeoSpatialFunctions.ShiftCoordinate(max, paddingMeters, paddingMeters)
        );
    }

    private static bool BoundsOverlap((Coordinate min, Coordinate max) left, (Coordinate min, Coordinate max) right)
    {
        return left.min.Lat <= right.max.Lat
            && left.max.Lat >= right.min.Lat
            && left.min.Lng <= right.max.Lng
            && left.max.Lng >= right.min.Lng;
    }

    private static bool Contains((Coordinate min, Coordinate max) bounds, Coordinate coordinate)
    {
        return coordinate.Lat >= bounds.min.Lat
            && coordinate.Lat <= bounds.max.Lat
            && coordinate.Lng >= bounds.min.Lng
            && coordinate.Lng <= bounds.max.Lng;
    }

    private static bool PointInPolygon(Coordinate point, IReadOnlyList<Coordinate> ring)
    {
        var inside = false;
        for (int current = 0, previous = ring.Count - 1; current < ring.Count; previous = current++)
        {
            var currentPoint = ring[current];
            var previousPoint = ring[previous];

            var intersects = ((currentPoint.Lat > point.Lat) != (previousPoint.Lat > point.Lat))
                && (point.Lng
                    < (previousPoint.Lng - currentPoint.Lng) * (point.Lat - currentPoint.Lat)
                        / ((previousPoint.Lat - currentPoint.Lat) + double.Epsilon)
                        + currentPoint.Lng);

            if (intersects)
            {
                inside = !inside;
            }
        }

        return inside;
    }
}