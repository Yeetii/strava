using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class GpxParserTests
{
    [Fact]
    public void TryParseRoute_ReturnsRouteForTrackPoints()
    {
        const string gpx = """
            <gpx version="1.1" creator="test">
              <trk>
                <name>Mont Blanc 50k</name>
                <trkseg>
                  <trkpt lat="45.9237" lon="6.8694" />
                  <trkpt lat="45.9240" lon="6.8701" />
                </trkseg>
              </trk>
            </gpx>
            """;

        var parsed = GpxParser.TryParseRoute(gpx);

        Assert.NotNull(parsed);
        Assert.Equal("Mont Blanc 50k", parsed!.Name);
        Assert.Equal(2, parsed.Coordinates.Count);
        Assert.Equal(6.8694, parsed.Coordinates[0].Lng, 4);
        Assert.Equal(45.9237, parsed.Coordinates[0].Lat, 4);
    }

    [Fact]
    public void TryParseRoute_UsesFallbackNameWhenNameMissing()
    {
        const string gpx = """
            <gpx version="1.1" creator="test">
              <rte>
                <rtept lat="46.0000" lon="7.0000" />
                <rtept lat="46.1000" lon="7.1000" />
              </rte>
            </gpx>
            """;

        var parsed = GpxParser.TryParseRoute(gpx, "Fallback race");

        Assert.NotNull(parsed);
        Assert.Equal("Fallback race", parsed!.Name);
    }

    [Fact]
    public void TryParseRoute_ReturnsNullForInvalidGpx()
    {
        var parsed = GpxParser.TryParseRoute("<not-gpx>");
        Assert.Null(parsed);
    }

    // ── CalculateDistanceKm ───────────────────────────────────────────────────

    [Fact]
    public void CalculateDistanceKm_ReturnsZeroForSinglePoint()
    {
        var coords = new[] { new Coordinate(6.8694, 45.9237) };
        Assert.Equal(0, GpxParser.CalculateDistanceKm(coords));
    }

    [Fact]
    public void CalculateDistanceKm_ReturnsZeroForEmptyList()
    {
        Assert.Equal(0, GpxParser.CalculateDistanceKm([]));
    }

    [Fact]
    public void CalculateDistanceKm_ComputesReasonableDistanceForKnownCoordinates()
    {
        // Stockholm (lng=18.0686, lat=59.3293) to Uppsala (lng=17.6389, lat=59.8586) is approx 64 km.
        var coords = new[]
        {
            new Coordinate(18.0686, 59.3293),
            new Coordinate(17.6389, 59.8586),
        };
        var distance = GpxParser.CalculateDistanceKm(coords);
        Assert.InRange(distance, 60, 70);
    }
}
