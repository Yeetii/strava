using BAMCIS.GeoJSON;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres.Spatial;
using Xunit;

namespace Stigar.Core.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class ProtectedAreaRepositoryTests(PostgisFixture fixture)
{
    private const string Polygon =
        """{"type":"Polygon","coordinates":[[[8.4,46.9],[8.6,46.9],[8.6,47.1],[8.4,47.1],[8.4,46.9]]]}""";

    [SkippableFact]
    public async Task GetById_ReadsPolygonGeometryAndProperties()
    {
        await fixture.ResetAsync();
        await InsertAreaAsync("area:1", "Alps Park", "national-park", "Q42", "Commons", """{"ref":"PA-1"}""");

        var area = await new ProtectedAreaRepository(fixture.DataSource).GetById("area:1");

        Assert.NotNull(area);
        Assert.Equal("area:1", area.Id);
        Assert.IsType<Polygon>(area.Geometry);
        Assert.Equal("Alps Park", area.Properties["name"]!.ToString());
        Assert.Equal("national-park", area.Properties["areaType"]!.ToString());
        Assert.Equal("Q42", area.Properties["wikidata"]!.ToString());
        Assert.Equal("Commons", area.Properties["wikimediaCommons"]!.ToString());
        Assert.Equal("PA-1", area.Properties["ref"]!.ToString());
        Assert.Equal(11, area.Zoom);
    }

    [SkippableFact]
    public async Task GetByIds_ReadsEveryArea()
    {
        await fixture.ResetAsync();
        await InsertAreaAsync("area:1", "A", "park", null, null, "{}");
        await InsertAreaAsync("area:2", "B", "park", null, null, "{}");

        var areas = await new ProtectedAreaRepository(fixture.DataSource).GetByIds(["area:1", "area:2"]);

        Assert.Equal(2, areas.Count);
        Assert.All(areas, area => Assert.IsType<Polygon>(area.Geometry));
    }

    [SkippableFact]
    public async Task FindContainingPoint_ReturnsAreaWithGeometry()
    {
        await fixture.ResetAsync();
        await InsertAreaAsync("area:1", "Alps Park", "park", null, null, "{}");

        var areas = await new ProtectedAreaRepository(fixture.DataSource)
            .FindContainingPoint(new Coordinate(8.5, 47.0));

        Assert.Equal("area:1", Assert.Single(areas).Id);
        Assert.IsType<Polygon>(Assert.Single(areas).Geometry);
    }

    [SkippableFact]
    public async Task FindSummariesContainingAnyPoint_ReadsDoubleCentroidColumns()
    {
        await fixture.ResetAsync();
        await InsertAreaAsync("area:1", "Alps Park", "park", null, null, "{}");

        var summaries = await new ProtectedAreaRepository(fixture.DataSource)
            .FindSummariesContainingAnyPoint([new Coordinate(8.5, 47.0)]);

        var summary = Assert.Single(summaries);
        Assert.Equal("area:1", summary.Id);
        Assert.NotNull(summary.Centroid);
        Assert.Equal(8.5, summary.Centroid!.Lng, 1);
        Assert.Equal(47.0, summary.Centroid.Lat, 1);
    }

    private Task InsertAreaAsync(
        string id,
        string name,
        string areaType,
        string? wikidata,
        string? commons,
        string properties)
        => fixture.ExecuteAsync(
            """
            INSERT INTO protected_areas
                (id, name, area_type, wikidata, wikimedia_commons, tile_x, tile_y, geom, properties)
            VALUES
                (@id, @name, @area_type, @wikidata, @commons, 542, 352,
                 ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326), @properties::jsonb)
            """,
            ("id", id), ("name", name), ("area_type", areaType),
            ("wikidata", wikidata), ("commons", commons),
            ("geom", Polygon), ("properties", properties));
}
