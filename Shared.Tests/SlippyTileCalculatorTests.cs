using Shared.Helpers;
using Shared.Models;

namespace Shared.Tests
{
    public class SlippyTileCalculatorTests
    {
        [Theory]
        [InlineData(0, 0, 0, 0, 0)]
        [InlineData(26.48928, 46.36592, 5, 18, 11)]
        [InlineData(-60.26080, -18.13080, 7, 42, 70)]
        public void WGS84ToTileIndex_ShouldReturnCorrectTileIndex(double lon, double lat, int zoom, int expectedTileX, int expectedTileY)
        {
            var coordinate = new Coordinate(lon, lat);
            var p = SlippyTileCalculator.WGS84ToTileIndex(coordinate, zoom);
            Assert.Equal(expectedTileX, p.X);
            Assert.Equal(expectedTileY, p.Y);

        }


        [Theory]
        [InlineData(0, 0, 0, -180, 85.05112878, 0, 0)]
        [InlineData(1, 1, 1, -90, 0, 0, -85.05112878)]
        [InlineData(7, 7, 3, 0, 0, 45, -45)]
        public void TileIndexToWGS84_ShouldReturnCorrectCoordinates(int x, int y, int z, double expectedNWLon, double expectedNWLat, double expectedSELon, double expectedSELat)
        {
            var (nw, se) = SlippyTileCalculator.TileIndexToWGS84(x, y, z);

            Assert.Equal(expectedNWLon, nw.Lng, 6);
            Assert.Equal(expectedNWLat, nw.Lat, 6);
            Assert.Equal(expectedSELon, se.Lng, 6);
            Assert.Equal(expectedSELat, se.Lat, 6);
        }
    }
}
