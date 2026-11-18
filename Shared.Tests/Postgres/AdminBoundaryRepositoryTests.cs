using BAMCIS.GeoJSON;
using Shared.Models;
using Shared.Services.Postgres.Spatial;
using Xunit;

namespace Shared.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class AdminBoundaryRepositoryTests(PostgisFixture fixture)
{
    private const string Polygon =
        """{"type":"Polygon","coordinates":[[[7.9,46.8],[8.1,46.8],[8.1,47.0],[7.9,47.0],[7.9,46.8]]]}""";

    [SkippableFact]
    public async Task GetById_ReadsTypedColumnsAndGeometry()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1", 1234567L, "relation", 4, "Canton", "Q99", 3);

        var boundary = await new AdminBoundaryRepository(fixture.DataSource).GetById("admin:1");

        Assert.NotNull(boundary);
        Assert.IsType<Polygon>(boundary.Geometry);
        Assert.Equal("1234567", boundary.Properties["osmId"]!.ToString());
        Assert.Equal("relation", boundary.Properties["osmType"]!.ToString());
        Assert.Equal("4", boundary.Properties["adminLevel"]!.ToString());
        Assert.Equal("Canton", boundary.Properties["name"]!.ToString());
        Assert.Equal("Q99", boundary.Properties["wikidata"]!.ToString());
        Assert.Equal("3", boundary.Properties["metricsVersion"]!.ToString());
    }

    [SkippableFact]
    public async Task GetByLevel_ReturnsOnlyMatchingBoundaries()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:4", 1L, "relation", 4, "Level 4", null, 0);
        await InsertBoundaryAsync("admin:6", 2L, "relation", 6, "Level 6", null, 0);

        var boundaries = await new AdminBoundaryRepository(fixture.DataSource).GetByLevel(6);

        var boundary = Assert.Single(boundaries);
        Assert.Equal("admin:6", boundary.Id);
        Assert.IsType<Polygon>(boundary.Geometry);
    }

    [SkippableFact]
    public async Task UpdateMetrics_Then_GetById_ReadsNewMetricsVersion()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1", 1L, "relation", 4, "Canton", null, 0);

        var repository = new AdminBoundaryRepository(fixture.DataSource);
        await repository.UpdateMetrics("admin:1", new Dictionary<string, object?> { ["population"] = 1000 }, 7);

        var boundary = await repository.GetById("admin:1");
        Assert.NotNull(boundary);
        Assert.Equal("7", boundary.Properties["metricsVersion"]!.ToString());
        Assert.Equal("1000", boundary.Properties["population"]!.ToString());
        Assert.IsType<Polygon>(boundary.Geometry);
    }

    [SkippableFact]
    public async Task FindSummariesContainingAnyPoint_ReadsDoubleCentroidColumns()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1", 1L, "relation", 4, "Canton", null, 0);

        var summaries = await new AdminBoundaryRepository(fixture.DataSource)
            .FindSummariesContainingAnyPoint([new Coordinate(8.0, 46.9)], 4);

        var summary = Assert.Single(summaries);
        Assert.Equal("admin:1", summary.Id);
        Assert.NotNull(summary.Centroid);
        Assert.Equal(8.0, summary.Centroid!.Lng, 1);
        Assert.Equal(46.9, summary.Centroid.Lat, 1);
    }

    private Task InsertBoundaryAsync(
        string id,
        long osmId,
        string osmType,
        int adminLevel,
        string name,
        string? wikidata,
        int metricsVersion)
        => fixture.ExecuteAsync(
            """
            INSERT INTO admin_boundaries
                (id, osm_id, osm_type, admin_level, name, wikidata, geom, properties, metrics_version)
            VALUES
                (@id, @osm_id, @osm_type, @admin_level, @name, @wikidata,
                 ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326), '{}'::jsonb, @metrics_version)
            """,
            ("id", id), ("osm_id", osmId), ("osm_type", osmType),
            ("admin_level", adminLevel), ("name", name), ("wikidata", wikidata),
            ("geom", Polygon), ("metrics_version", metricsVersion));
}
