using System.Drawing;
using Shared.Models;

namespace Shared.Helpers
{
    public class SlippyTileCalculator
    {
        public static Point WGS84ToTileIndex(Coordinate coordinate, int zoom)
        {
            var lon = coordinate.Lng;
            var lat = coordinate.Lat;
            Point p = new()
            {
                X = (int)((lon + 180.0) / 360.0 * (1 << zoom)),
                Y = (int)((1.0 - Math.Log(Math.Tan(lat * Math.PI / 180.0) +
                1.0 / Math.Cos(lat * Math.PI / 180.0)) / Math.PI) / 2.0 * (1 << zoom))
            };

            return p;
        }
        public static (Coordinate nw, Coordinate se) TileIndexToWGS84(int x, int y, int z)
        {
            double nwLon = TileXToLon(x, z);
            double nwLat = TileYToLat(y, z);

            double seLon = TileXToLon(x + 1, z);
            double seLat = TileYToLat(y + 1, z);

            var nw = new Coordinate(nwLon, nwLat);
            var se = new Coordinate(seLon, seLat);

            return (nw, se);
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