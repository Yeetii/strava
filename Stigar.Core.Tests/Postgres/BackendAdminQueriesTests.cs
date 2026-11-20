using Stigar.Core.Services.Postgres.Admin;
using Xunit;

namespace Stigar.Core.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class BackendAdminQueriesTests(PostgisFixture fixture)
{
    private const string Polygon =
        """{"type":"Polygon","coordinates":[[[7.9,46.8],[8.1,46.8],[8.1,47.0],[7.9,47.0],[7.9,46.8]]]}""";

    [SkippableFact]
    public async Task GetStaleAdminBoundaryIds_FirstPageWithNullAfterId_ReturnsStaleIds()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1", metricsVersion: 3);
        await InsertBoundaryAsync("admin:2", metricsVersion: 7);

        var ids = await new BackendAdminQueries(fixture.DataSource)
            .GetStaleAdminBoundaryIds(metricsVersion: 7, afterId: null, limit: 100);

        Assert.Equal(["admin:1"], ids);
    }

    [SkippableFact]
    public async Task GetStaleAdminBoundaryIds_WithAfterId_ContinuesKeysetPaging()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1", metricsVersion: 3);
        await InsertBoundaryAsync("admin:2", metricsVersion: 3);
        await InsertBoundaryAsync("admin:3", metricsVersion: 3);

        var queries = new BackendAdminQueries(fixture.DataSource);
        var firstPage = await queries.GetStaleAdminBoundaryIds(7, afterId: null, limit: 2);
        var secondPage = await queries.GetStaleAdminBoundaryIds(7, afterId: firstPage[^1], limit: 2);

        Assert.Equal(["admin:1", "admin:2"], firstPage);
        Assert.Equal(["admin:3"], secondPage);
    }

    [SkippableFact]
    public async Task GetActivityIdsWithMissingOsmHighwayId_FirstPageWithNullAfterId_ReturnsIds()
    {
        await fixture.ResetAsync();
        await InsertPathAsync("activity:1", "");
        await InsertPathAsync("activity:2", "way/2");
        await InsertPathAsync("activity:3", "");

        var ids = await new BackendAdminQueries(fixture.DataSource)
            .GetActivityIdsWithMissingOsmHighwayId(afterActivityId: null, limit: 100);

        Assert.Equal(["activity:1", "activity:3"], ids);
    }

    [SkippableFact]
    public async Task GetActivityIdsWithMissingOsmHighwayId_WithAfterId_ContinuesKeysetPaging()
    {
        await fixture.ResetAsync();
        await InsertPathAsync("activity:1", "");
        await InsertPathAsync("activity:2", "");
        await InsertPathAsync("activity:3", "");

        var queries = new BackendAdminQueries(fixture.DataSource);
        var firstPage = await queries.GetActivityIdsWithMissingOsmHighwayId(null, limit: 2);
        var secondPage = await queries.GetActivityIdsWithMissingOsmHighwayId(firstPage[^1], limit: 2);

        Assert.Equal(["activity:1", "activity:2"], firstPage);
        Assert.Equal(["activity:3"], secondPage);
    }

    private Task InsertBoundaryAsync(string id, int metricsVersion)
        => fixture.ExecuteAsync(
            """
            INSERT INTO admin_boundaries
                (id, osm_id, osm_type, admin_level, name, geom, properties, metrics_version)
            VALUES
                (@id, 1, 'relation', 4, 'Stale', ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326),
                 '{}'::jsonb, @metrics_version)
            """,
            ("id", id), ("geom", Polygon), ("metrics_version", metricsVersion));

    private async Task InsertPathAsync(string activityId, string osmHighwayId)
    {
        await fixture.ExecuteAsync("INSERT INTO users (id) VALUES ('user:1') ON CONFLICT DO NOTHING");
        await fixture.ExecuteAsync(
            """
            INSERT INTO activities (id, user_id, name, sport_type, start_date, start_date_local)
            VALUES (@id, 'user:1', 'Run', 'Run', NOW(), NOW())
            ON CONFLICT DO NOTHING
            """,
            ("id", activityId));
        await fixture.ExecuteAsync(
            """
            INSERT INTO activity_visited_paths (activity_id, user_id, osm_highway_id)
            VALUES (@activity_id, 'user:1', @osm_highway_id)
            """,
            ("activity_id", activityId), ("osm_highway_id", osmHighwayId));
    }
}
