using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services.Postgres.Spatial;
using Xunit;

namespace Shared.Tests.Postgres;

/// <summary>
/// Exercises the real SQL → NpgsqlDataReader → Map() path. The GetByTiles regression was a
/// projection that emitted doubles where Map() expected GeoJSON text; a mocked repository
/// could never detect that.
/// </summary>
[Collection(PostgisCollection.Name)]
public sealed class PeakRepositoryTests(PostgisFixture fixture)
{
    private const int TileX = 542;
    private const int TileY = 352;
    private const double Lng = 8.5384;
    private const double Lat = 46.9601;

    [SkippableFact]
    public async Task GetByTiles_ReadsGeoJsonPointAndProperties()
    {
        await fixture.ResetAsync();
        await InsertPeakAsync("peak:1", Lng, Lat, TileX, TileY, "Test Peak", 1500f);

        var peaks = await new PeakRepository(fixture.DataSource).GetByTiles([(TileX, TileY)]);

        var peak = Assert.Single(peaks);
        var point = Assert.IsType<Point>(peak.Geometry);
        Assert.Equal(Lng, point.Coordinates.Longitude, 6);
        Assert.Equal(Lat, point.Coordinates.Latitude, 6);
        Assert.Equal("Test Peak", peak.Properties["name"]!.ToString());
        Assert.Equal("1500", peak.Properties["ele"]!.ToString());
        Assert.Equal(TileX, peak.X);
        Assert.Equal(TileY, peak.Y);
        Assert.Equal(SpatialRowMappingPeakZoom, peak.Zoom);
    }

    [SkippableFact]
    public async Task GetByTiles_ReturnsOnlyPeaksInRequestedTiles()
    {
        await fixture.ResetAsync();
        await InsertPeakAsync("peak:1", Lng, Lat, TileX, TileY, "Inside", null);
        await InsertPeakAsync("peak:2", Lng + 0.1, Lat + 0.1, TileX + 1, TileY, "Outside", null);

        var peaks = await new PeakRepository(fixture.DataSource).GetByTiles([(TileX, TileY)]);

        var peak = Assert.Single(peaks);
        Assert.Equal("peak:1", peak.Id);
        Assert.Equal("Inside", peak.Properties["name"]!.ToString());
    }

    [SkippableFact]
    public async Task GetByTiles_WithSeveralTiles_ReturnsAllMatchingPeaks()
    {
        await fixture.ResetAsync();
        await InsertPeakAsync("peak:1", Lng, Lat, TileX, TileY, "A", null);
        await InsertPeakAsync("peak:2", Lng + 0.1, Lat + 0.1, TileX + 1, TileY, "B", null);

        var peaks = await new PeakRepository(fixture.DataSource).GetByTiles([(TileX, TileY), (TileX + 1, TileY)]);

        Assert.Equal(2, peaks.Count);
        Assert.Equal(["peak:1", "peak:2"], peaks.Select(p => p.Id).OrderBy(id => id));
    }

    [SkippableFact]
    public async Task GetById_ReadsGeometryAndProperties()
    {
        await fixture.ResetAsync();
        await InsertPeakAsync("peak:1", Lng, Lat, TileX, TileY, "Solo", 1234.5f);

        var peak = await new PeakRepository(fixture.DataSource).GetById("peak:1");

        Assert.NotNull(peak);
        Assert.Equal("Solo", peak.Properties["name"]!.ToString());
        Assert.Equal("1234.5", peak.Properties["ele"]!.ToString());
        var point = Assert.IsType<Point>(peak.Geometry);
        Assert.Equal(Lng, point.Coordinates.Longitude, 6);
        Assert.Equal(Lat, point.Coordinates.Latitude, 6);
    }

    [SkippableFact]
    public async Task GetByIds_ReadsAllRequestedPeaks()
    {
        await fixture.ResetAsync();
        await InsertPeakAsync("peak:1", Lng, Lat, TileX, TileY, "A", null);
        await InsertPeakAsync("peak:2", Lng + 0.01, Lat, TileX, TileY, "B", null);

        var peaks = await new PeakRepository(fixture.DataSource).GetByIds(["peak:1", "peak:2"]);

        Assert.Equal(2, peaks.Count);
        Assert.All(peaks, peak => Assert.IsType<Point>(peak.Geometry));
    }

    [SkippableFact]
    public async Task FindWithinRadius_ReturnsOnlyNearbyPeaks()
    {
        await fixture.ResetAsync();
        await InsertPeakAsync("peak:near", Lng, Lat, TileX, TileY, "Near", null);
        await InsertPeakAsync("peak:far", Lng + 1, Lat, TileX + 1, TileY, "Far", null);

        var peaks = await new PeakRepository(fixture.DataSource)
            .FindWithinRadius(new Coordinate(Lng, Lat), 500);

        var peak = Assert.Single(peaks);
        Assert.Equal("peak:near", peak.Id);
        Assert.IsType<Point>(peak.Geometry);
    }

    [SkippableFact]
    public async Task FindContainingPoint_ReturnsMatchingPeak()
    {
        await fixture.ResetAsync();
        await InsertPeakAsync("peak:1", Lng, Lat, TileX, TileY, "Exact", null);

        var peaks = await new PeakRepository(fixture.DataSource)
            .FindContainingPoint(new Coordinate(Lng, Lat));

        Assert.Equal("peak:1", Assert.Single(peaks).Id);
    }

    [SkippableFact]
    public async Task Upsert_Then_GetById_RoundTrips()
    {
        await fixture.ResetAsync();
        var peak = new StoredFeature
        {
            Id = "peak:written",
            FeatureId = "written",
            Kind = Shared.Services.FeatureKinds.Peak,
            X = TileX,
            Y = TileY,
            Zoom = 11,
            Geometry = new Point(new Position(Lng, Lat)),
            Properties = new Dictionary<string, dynamic> { ["name"] = "Written", ["ele"] = "2000" },
        };

        var repository = new PeakRepository(fixture.DataSource);
        await repository.Upsert(peak);

        var read = await repository.GetById("peak:written");
        Assert.NotNull(read);
        Assert.Equal("Written", read.Properties["name"]!.ToString());
        Assert.Equal("2000", read.Properties["ele"]!.ToString());
        Assert.Equal(Lng, Assert.IsType<Point>(read.Geometry).Coordinates.Longitude, 6);
    }

    // Kept local so the test pins the production peak zoom without reaching into internal helpers.
    private const int SpatialRowMappingPeakZoom = 11;

    private Task InsertPeakAsync(
        string id,
        double lng,
        double lat,
        int x,
        int y,
        string name,
        float? elevation)
        => fixture.ExecuteAsync(
            """
            INSERT INTO peaks (id, name, elevation, tile_x, tile_y, geom)
            VALUES (@id, @name, @elevation, @x, @y, ST_SetSRID(ST_MakePoint(@lng, @lat), 4326))
            """,
            ("id", id), ("name", name), ("elevation", elevation),
            ("x", x), ("y", y), ("lng", lng), ("lat", lat));
}
