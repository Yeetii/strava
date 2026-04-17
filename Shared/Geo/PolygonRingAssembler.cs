using BAMCIS.GeoJSON;

namespace Shared.Geo;

/// <summary>
/// Assembles open polyline segments (OSM way geometries) into closed polygon rings.
/// OSM relation outer boundaries are composed of multiple connected way segments
/// that must be joined into complete rings before being used as polygon boundaries.
/// </summary>
public static class PolygonRingAssembler
{
    private readonly record struct PosKey(long Lat, long Lon)
    {
        public static PosKey From(Position p) => new(
            (long)Math.Round(p.Latitude * 1e7),
            (long)Math.Round(p.Longitude * 1e7));
    }

    /// <summary>
    /// Assembles a collection of open polyline segments into closed <see cref="LinearRing"/> instances
    /// by connecting segments where their endpoints match. Segments may be reversed if needed.
    /// Uses an endpoint index for O(1) lookups instead of scanning all segments.
    /// </summary>
    public static IEnumerable<LinearRing> AssembleRings(IEnumerable<IEnumerable<Position>> segments)
    {
        var segs = segments.Select(s => s.ToList()).ToArray();
        var consumed = new bool[segs.Length];

        // Endpoint index: maps rounded position → segments with that endpoint
        var index = new Dictionary<PosKey, List<(int SegIndex, bool IsStart)>>();
        for (int i = 0; i < segs.Length; i++)
        {
            GetOrCreate(index, PosKey.From(segs[i][0])).Add((i, true));
            GetOrCreate(index, PosKey.From(segs[i][^1])).Add((i, false));
        }

        for (int seed = 0; seed < segs.Length; seed++)
        {
            if (consumed[seed]) continue;
            Consume(index, segs, consumed, seed);

            var ring = new LinkedList<Position>(segs[seed]);

            while (!IsRingClosed(ring))
            {
                // Try to extend from the ring's tail
                var match = FindMatch(index, PosKey.From(ring.Last!.Value), consumed);
                if (match is { } tail)
                {
                    Consume(index, segs, consumed, tail.SegIndex);
                    var seg = segs[tail.SegIndex];
                    if (tail.IsStart)
                        foreach (var p in seg.Skip(1)) ring.AddLast(p);
                    else
                        for (int j = seg.Count - 2; j >= 0; j--) ring.AddLast(seg[j]);
                    continue;
                }

                // Try to extend from the ring's head
                match = FindMatch(index, PosKey.From(ring.First!.Value), consumed);
                if (match is { } head)
                {
                    Consume(index, segs, consumed, head.SegIndex);
                    var seg = segs[head.SegIndex];
                    if (head.IsStart)
                        foreach (var p in seg.Skip(1)) ring.AddFirst(p);
                    else
                        for (int j = seg.Count - 2; j >= 0; j--) ring.AddFirst(seg[j]);
                    continue;
                }

                break;
            }

            var ringList = ring.ToList();
            if (!IsRingClosed(ringList))
                ringList.Add(new Position(ringList[0].Longitude, ringList[0].Latitude));

            if (ringList.Count >= 4)
                yield return new LinearRing(ringList, null);
        }
    }

    private static (int SegIndex, bool IsStart)? FindMatch(
        Dictionary<PosKey, List<(int SegIndex, bool IsStart)>> index,
        PosKey key,
        bool[] consumed)
    {
        if (!index.TryGetValue(key, out var entries)) return null;
        foreach (var entry in entries)
            if (!consumed[entry.SegIndex]) return entry;
        return null;
    }

    private static void Consume(
        Dictionary<PosKey, List<(int SegIndex, bool IsStart)>> index,
        List<Position>[] segs,
        bool[] consumed,
        int segIdx)
    {
        consumed[segIdx] = true;
        RemoveEntry(index, PosKey.From(segs[segIdx][0]), segIdx);
        RemoveEntry(index, PosKey.From(segs[segIdx][^1]), segIdx);
    }

    private static void RemoveEntry(
        Dictionary<PosKey, List<(int SegIndex, bool IsStart)>> index,
        PosKey key,
        int segIdx)
    {
        if (!index.TryGetValue(key, out var entries)) return;
        entries.RemoveAll(e => e.SegIndex == segIdx);
        if (entries.Count == 0) index.Remove(key);
    }

    private static List<T> GetOrCreate<T>(Dictionary<PosKey, List<T>> dict, PosKey key)
    {
        if (!dict.TryGetValue(key, out var list))
        {
            list = [];
            dict[key] = list;
        }
        return list;
    }

    private static bool IsRingClosed(LinkedList<Position> ring)
    {
        if (ring.Count < 2) return false;
        var first = ring.First!.Value;
        var last = ring.Last!.Value;
        return Math.Abs(first.Latitude - last.Latitude) < 1e-7
            && Math.Abs(first.Longitude - last.Longitude) < 1e-7;
    }

    private static bool IsRingClosed(List<Position> ring)
    {
        if (ring.Count < 2) return false;
        return Math.Abs(ring[0].Latitude - ring[^1].Latitude) < 1e-7
            && Math.Abs(ring[0].Longitude - ring[^1].Longitude) < 1e-7;
    }
}
