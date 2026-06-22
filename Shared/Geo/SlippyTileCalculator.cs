using Shared.Models;

namespace Shared.Geo
{
    public class SlippyTileCalculator
    {
        private const int DefaultZoom = 11;

        public static IEnumerable<(int x, int y)> TileIndicesByLine(IEnumerable<Coordinate> line, int zoom = DefaultZoom)
        {
            foreach (var coord in line)
            {
                yield return WGS84ToTileIndex(coord, zoom);
            }
        }

        public static (int x, int y) WGS84ToTileIndex(Coordinate coordinate, int zoom = DefaultZoom)
        {
            var lon = coordinate.Lng;
            var lat = coordinate.Lat;
            var tileCount = 1 << zoom;
            var x = (int)Math.Floor((lon + 180.0) / 360.0 * tileCount);
            var y = (int)Math.Floor((1.0 - Math.Log(Math.Tan(lat * Math.PI / 180.0) +
                1.0 / Math.Cos(lat * Math.PI / 180.0)) / Math.PI) / 2.0 * tileCount);

            return (Math.Clamp(x, 0, tileCount - 1), Math.Clamp(y, 0, tileCount - 1));
        }
        public static (Coordinate sw, Coordinate ne) TileIndexToWGS84(int x, int y, int z = DefaultZoom)
        {
            double wLon = TileXToLon(x, z);
            double nLat = TileYToLat(y, z);

            double eLon = TileXToLon(x + 1, z);
            double sLat = TileYToLat(y + 1, z);

            var sw = new Coordinate(wLon, sLat);
            var ne = new Coordinate(eLon, nLat);

            return (sw, ne);
        }

        private static double TileXToLon(int x, int z)
        {
            return x / Math.Pow(2.0, z) * 360.0 - 180.0;
        }

        private static double TileYToLat(int y, int z)
        {
            double n = Math.PI - 2.0 * Math.PI * y / Math.Pow(2.0, z);
            return Math.Atan(Math.Sinh(n)) * 180.0 / Math.PI;
        }

        /// <summary>
        /// Returns the tile keys at <paramref name="targetZoom"/> that intersect the given tile.
        /// When z == targetZoom returns exactly [(x, y)]; when z &gt; targetZoom returns the
        /// single parent tile; when z &lt; targetZoom expands to all child tiles.
        /// </summary>
        public static IReadOnlyList<(int x, int y)> GetIntersectingTileKeys(int z, int x, int y, int targetZoom)
        {
            if (z == targetZoom)
                return [(x, y)];

            if (z > targetZoom)
            {
                var scale = 1 << (z - targetZoom);
                return [(x / scale, y / scale)];
            }

            var expansion = 1 << (targetZoom - z);
            var minX = x * expansion;
            var minY = y * expansion;
            var result = new List<(int x, int y)>(expansion * expansion);
            for (var currentX = minX; currentX < minX + expansion; currentX++)
                for (var currentY = minY; currentY < minY + expansion; currentY++)
                    result.Add((currentX, currentY));
            return result;
        }
    }
}
