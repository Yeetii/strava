using Shared.Geo;
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
            Assert.Equal(expectedTileX, p.x);
            Assert.Equal(expectedTileY, p.y);

        }

        [Theory]
        [InlineData(0, 0, 0)]
        [InlineData(1, 1, 1)]
        [InlineData(7, 7, 3)]
        public void TileIndexToWGS84_ShouldReturnNwCornerInSameTile(int x, int y, int z)
        {
            var (nw, se) = SlippyTileCalculator.TileIndexToWGS84(x, y, z);
            var (x2, y2) = SlippyTileCalculator.WGS84ToTileIndex(nw, z);
            Assert.Equal(x, x2);
            Assert.Equal(y, y2);
        }
    }
}
