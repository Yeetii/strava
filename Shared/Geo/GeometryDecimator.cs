using BAMCIS.GeoJSON;

namespace Shared.Geo;

public static class GeometryDecimator
{
    /// <summary>
    /// Simplifies a geometry using the Ramer-Douglas-Peucker algorithm.
    /// <paramref name="epsilon"/> is the maximum perpendicular distance (in degrees)
    /// a point may deviate from the simplified line before it is kept.
    /// For admin_level=2 at zoom 6, 0.01° (~1 km) is a good default.
    /// </summary>
    public static Geometry Simplify(Geometry geometry, double epsilon)
    {
        if (epsilon <= 0)
            return geometry;

        return geometry switch
        {
            Polygon polygon => SimplifyPolygon(polygon, epsilon),
            MultiPolygon multiPolygon => SimplifyMultiPolygon(multiPolygon, epsilon),
            _ => geometry
        };
    }

    /// <summary>
    /// Legacy nth-point decimation. Kept as a last-resort fallback when a
    /// simplified geometry still exceeds Cosmos document size limits.
    /// </summary>
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

    // --- RDP simplification ---------------------------------------------------

    private static Polygon SimplifyPolygon(Polygon polygon, double epsilon)
    {
        var rings = polygon.Coordinates
            .Select(ring => SimplifyRing(ring, epsilon))
            .Where(ring => ring != null)
            .Cast<LinearRing>()
            .ToList();

        return rings.Count == 0 ? polygon : new Polygon(rings, null);
    }

    private static Geometry SimplifyMultiPolygon(MultiPolygon multiPolygon, double epsilon)
    {
        var polygons = multiPolygon.Coordinates
            .Select(polygon => SimplifyPolygon(polygon, epsilon))
            .ToList();

        return polygons.Count switch
        {
            0 => multiPolygon,
            1 => polygons[0],
            _ => new MultiPolygon(polygons, null)
        };
    }

    private static LinearRing? SimplifyRing(LinearRing ring, double epsilon)
    {
        var positions = ring.Coordinates.ToList();
        if (positions.Count < 4)
            return ring;

        // Work with the open ring (drop closing duplicate).
        var open = positions.Take(positions.Count - 1).ToList();

        var simplified = RamerDouglasPeucker(open, epsilon);

        if (simplified.Count < 3)
            return ring;

        // Close the ring.
        simplified.Add(new Position(simplified[0].Longitude, simplified[0].Latitude));
        return new LinearRing(simplified, null);
    }

    /// <summary>
    /// Ramer-Douglas-Peucker line simplification (iterative to avoid stack
    /// overflow on rings with 100k+ points).
    /// </summary>
    private static List<Position> RamerDouglasPeucker(List<Position> points, double epsilon)
    {
        if (points.Count < 3)
            return new List<Position>(points);

        var keep = new bool[points.Count];
        keep[0] = true;
        keep[points.Count - 1] = true;

        // Explicit stack instead of recursion.
        var stack = new Stack<(int start, int end)>();
        stack.Push((0, points.Count - 1));

        while (stack.Count > 0)
        {
            var (start, end) = stack.Pop();
            if (end - start < 2)
                continue;

            double maxDist = 0;
            int index = start;

            for (int i = start + 1; i < end; i++)
            {
                double dist = PerpendicularDistance(points[i], points[start], points[end]);
                if (dist > maxDist)
                {
                    maxDist = dist;
                    index = i;
                }
            }

            if (maxDist > epsilon)
            {
                keep[index] = true;
                stack.Push((start, index));
                stack.Push((index, end));
            }
        }

        var result = new List<Position>();
        for (int i = 0; i < points.Count; i++)
        {
            if (keep[i])
                result.Add(points[i]);
        }

        return result;
    }

    /// <summary>
    /// Perpendicular distance of <paramref name="point"/> from the line
    /// defined by <paramref name="lineStart"/> and <paramref name="lineEnd"/>,
    /// measured in the same unit as the coordinates (degrees).
    /// </summary>
    private static double PerpendicularDistance(Position point, Position lineStart, Position lineEnd)
    {
        double dx = lineEnd.Longitude - lineStart.Longitude;
        double dy = lineEnd.Latitude - lineStart.Latitude;

        double lengthSq = dx * dx + dy * dy;
        if (lengthSq == 0)
        {
            // lineStart == lineEnd — just return distance to that point.
            double px = point.Longitude - lineStart.Longitude;
            double py = point.Latitude - lineStart.Latitude;
            return Math.Sqrt(px * px + py * py);
        }

        // Project point onto line, clamped to [0,1].
        double t = ((point.Longitude - lineStart.Longitude) * dx + (point.Latitude - lineStart.Latitude) * dy) / lengthSq;
        t = Math.Clamp(t, 0, 1);

        double projX = lineStart.Longitude + t * dx;
        double projY = lineStart.Latitude + t * dy;

        double ex = point.Longitude - projX;
        double ey = point.Latitude - projY;
        return Math.Sqrt(ex * ex + ey * ey);
    }

    // --- Legacy nth-point decimation ------------------------------------------

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