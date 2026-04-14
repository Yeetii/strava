using BAMCIS.GeoJSON;

namespace Shared.Geo;

public static class GeometryDecimator
{
    public static Geometry Decimate(Geometry geometry, int step)
    {
        if (step <= 1)
            return geometry;

        return geometry switch
        {
            Polygon polygon => DecimatePolygon(polygon, step),
            MultiPolygon multiPolygon => DecimateMultiPolygon(multiPolygon, step),
            _ => geometry
        };
    }

    private static Polygon DecimatePolygon(Polygon polygon, int step)
    {
        var rings = polygon.Coordinates
            .Select(ring => DecimateRing(ring, step))
            .Where(ring => ring != null)
            .Cast<LinearRing>()
            .ToList();

        return rings.Count == 0 ? polygon : new Polygon(rings, null);
    }

    private static Geometry DecimateMultiPolygon(MultiPolygon multiPolygon, int step)
    {
        var polygons = multiPolygon.Coordinates
            .Select(polygon => DecimatePolygon(polygon, step))
            .ToList();

        return polygons.Count switch
        {
            0 => multiPolygon,
            1 => polygons[0],
            _ => new MultiPolygon(polygons, null)
        };
    }

    private static LinearRing? DecimateRing(LinearRing ring, int step)
    {
        var positions = ring.Coordinates.ToList();
        if (positions.Count < 4)
            return ring;

        var openPositions = positions.Take(positions.Count - 1).ToList();
        var decimated = new List<Position>();
        for (var index = 0; index < openPositions.Count; index += step)
        {
            decimated.Add(openPositions[index]);
        }

        var last = openPositions[^1];
        if (!PositionEquals(decimated[^1], last))
            decimated.Add(last);

        if (decimated.Count < 3)
            return ring;

        decimated.Add(new Position(decimated[0].Longitude, decimated[0].Latitude));
        return new LinearRing(decimated, null);
    }

    private static bool PositionEquals(Position left, Position right)
    {
        const double tolerance = 1e-9;
        return Math.Abs(left.Longitude - right.Longitude) < tolerance
            && Math.Abs(left.Latitude - right.Latitude) < tolerance;
    }
}