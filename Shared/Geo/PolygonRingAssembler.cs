using BAMCIS.GeoJSON;

namespace Shared.Geo;

/// <summary>
/// Assembles open polyline segments (OSM way geometries) into closed polygon rings.
/// OSM relation outer boundaries are composed of multiple connected way segments
/// that must be joined into complete rings before being used as polygon boundaries.
/// </summary>
public static class PolygonRingAssembler
{
    /// <summary>
    /// Assembles a collection of open polyline segments into closed <see cref="LinearRing"/> instances
    /// by connecting segments where their endpoints match. Segments may be reversed if needed.
    /// </summary>
    public static IEnumerable<LinearRing> AssembleRings(IEnumerable<IEnumerable<Position>> segments)
    {
        var remaining = segments.Select(s => s.ToList()).ToList();

        while (remaining.Count > 0)
        {
            var ring = new LinkedList<Position>(remaining[0]);
            remaining.RemoveAt(0);

            bool extended = true;
            while (extended && !IsRingClosed(ring))
            {
                extended = false;
                for (int i = 0; i < remaining.Count; i++)
                {
                    var seg = remaining[i];

                    // Try appending to the end of the ring.
                    if (PositionsAreEqual(ring.Last!.Value, seg[0]))
                    {
                        foreach (var p in seg.Skip(1)) ring.AddLast(p);
                        remaining.RemoveAt(i);
                        extended = true;
                        break;
                    }
                    if (PositionsAreEqual(ring.Last!.Value, seg[^1]))
                    {
                        seg.Reverse();
                        foreach (var p in seg.Skip(1)) ring.AddLast(p);
                        remaining.RemoveAt(i);
                        extended = true;
                        break;
                    }

                    // Try prepending to the start of the ring.
                    if (PositionsAreEqual(ring.First!.Value, seg[^1]))
                    {
                        foreach (var p in seg.Take(seg.Count - 1).Reverse()) ring.AddFirst(p);
                        remaining.RemoveAt(i);
                        extended = true;
                        break;
                    }
                    if (PositionsAreEqual(ring.First!.Value, seg[0]))
                    {
                        foreach (var p in seg.Skip(1)) ring.AddFirst(p);
                        remaining.RemoveAt(i);
                        extended = true;
                        break;
                    }
                }
            }

            var ringList = ring.ToList();
            if (!IsRingClosed(ringList))
                ringList.Add(new Position(ringList[0].Longitude, ringList[0].Latitude));

            if (ringList.Count >= 4)
                yield return new LinearRing(ringList, null);
        }
    }

    private static bool IsRingClosed(LinkedList<Position> ring)
    {
        return ring.Count >= 2 && PositionsAreEqual(ring.First!.Value, ring.Last!.Value);
    }

    private static bool IsRingClosed(List<Position> ring)
    {
        return ring.Count >= 2 && PositionsAreEqual(ring[0], ring[^1]);
    }

    private static bool PositionsAreEqual(Position a, Position b)
    {
        const double tolerance = 1e-7;
        return Math.Abs(a.Latitude - b.Latitude) < tolerance
            && Math.Abs(a.Longitude - b.Longitude) < tolerance;
    }
}
