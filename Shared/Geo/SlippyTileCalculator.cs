using System.Drawing;
using System.Linq.Expressions;
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
            var x = (int)((lon + 180.0) / 360.0 * (1 << zoom));
            var y = (int)((1.0 - Math.Log(Math.Tan(lat * Math.PI / 180.0) +
                1.0 / Math.Cos(lat * Math.PI / 180.0)) / Math.PI) / 2.0 * (1 << zoom));

            return (x, y);
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
    }
}