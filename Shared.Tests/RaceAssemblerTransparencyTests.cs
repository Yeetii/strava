using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services;

namespace Shared.Tests;

public class RaceAssemblerTransparencyTests
{
    [Fact]
    public void CreateTransparencyMarker_DowngradesLineStringToPoint()
    {
        var race = new StoredFeature
        {
            Id = "race:sample-0",
            FeatureId = "sample-0",
            Kind = FeatureKinds.Race,
            X = 12,
            Y = 34,
            Zoom = RaceCollectionClient.DefaultZoom,
            Geometry = new LineString([
                new Position(10, 20),
                new Position(30, 40),
            ]),
            Properties = new Dictionary<string, dynamic>
            {
                ["name"] = "Sample race",
            },
        };

        var marker = RaceAssembler.CreateTransparencyMarker(race);

        var point = Assert.IsType<Point>(marker.Geometry);
        Assert.Equal(20, point.Coordinates.Longitude);
        Assert.Equal(30, point.Coordinates.Latitude);
        Assert.Equal(race.Id, marker.Id);
        Assert.Equal(race.FeatureId, marker.FeatureId);
        Assert.Equal(race.X, marker.X);
        Assert.Equal(race.Y, marker.Y);
        Assert.Equal("Sample race", marker.Properties["name"]);
    }
}