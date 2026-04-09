using BAMCIS.GeoJSON;
using Shared.Models;

namespace Shared.Geo;

public static class GeometryCentroidHelper
{
    public static Coordinate GetCentroid(Geometry geometry)
    {
        return geometry switch
            {
                Point point => new Coordinate(point.Coordinates.Longitude, point.Coordinates.Latitude),
                LineString lineString => GetLineCentroid(lineString),
                Polygon polygon => GetPolygonCentroid(polygon),
                MultiPolygon multiPolygon => GetPolygonCentroid(multiPolygon.Coordinates.First()),
                _ => throw new NotSupportedException($"Geometry type {geometry.Type} not supported for tile calculation")
            };
    }

    public static Coordinate GetLineCentroid(LineString line)
    {
        // Midpoint of the line (by number of points, not length)
        var coords = line.Coordinates;
        int count = coords.Count();
        if (count == 0) return new Coordinate(0, 0);
        if (count % 2 == 1)
        {
            // Odd: return the middle point
            var mid = coords.ElementAt(count / 2);
            return new Coordinate(mid.Latitude, mid.Longitude);
        }
        else
        {
            // Even: average the two central points
            var mid1 = coords.ElementAt((count / 2) - 1);
            var mid2 = coords.ElementAt(count / 2);
            return new Coordinate((mid1.Latitude + mid2.Latitude) / 2, (mid1.Longitude + mid2.Longitude) / 2);
        }
    }

    public static Coordinate GetPolygonCentroid(Polygon polygon)
    {
        var coords = polygon.Coordinates.First();
        double sumLat = 0, sumLng = 0;
        int count = coords.Coordinates.Count();
        foreach (var pos in coords.Coordinates)
        {
            sumLat += pos.Latitude;
            sumLng += pos.Longitude;
        }
        return new Coordinate(sumLat / count, sumLng / count);
    }
}
