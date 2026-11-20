using Stigar.Core.Services.Postgres.Admin;
using Xunit;

namespace Stigar.Core.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class BackendAdminQueriesTests(PostgisFixture fixture)
{
    private const string Polygon =
        """{"type":"Polygon","coordinates":[[[7.9,46.8],[8.1,46.8],[8.1,47.0],[7.9,47.0],[7.9,46.8]]]}""";

    [SkippableFact]
    public async Task GetStaleAdminBoundaryIds_SelectsRowsBehindTheEpoch()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1", generation: 0);
        await InsertBoundaryAsync("admin:2", generation: 2);

        var ids = await new BackendAdminQueries(fixture.DataSource)
            .GetStaleAdminBoundaryIds(epoch: 2, afterId: null, limit: 100);

        Assert.Equal(["admin:1"], ids);
    }

    [SkippableFact]
    public async Task GetStaleAdminBoundaryIds_SkipsRecentlyQueuedBoundaries()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1");
        await InsertBoundaryAsync("admin:2");
        await fixture.ExecuteAsync(
            "UPDATE admin_boundaries SET metrics_queued_at = NOW() WHERE id = 'admin:2'");

        var ids = await new BackendAdminQueries(fixture.DataSource)
            .GetStaleAdminBoundaryIds(epoch: 1, afterId: null, limit: 100);

        Assert.Equal(["admin:1"], ids);
    }

    [SkippableFact]
    public async Task GetActivityIdsNeedingProcessing_SelectsStagesBehindTheEpoch()
    {
        await fixture.ResetAsync();
        await InsertActivityAsync("activity:1");
        await InsertActivityAsync("activity:2");
        await InsertActivityAsync("activity:3");
        foreach (var key in new[] { "summitedPeaksEpoch", "visitedPathsEpoch", "visitedAreasEpoch" })
        {
            await SetStageEpochAsync("activity:2", key, 1);
            await SetStageEpochAsync("activity:3", key, 2);
        }

        var ids = await new BackendAdminQueries(fixture.DataSource)
            .GetActivityIdsNeedingProcessing(2, 2, 2, limit: 100);

        Assert.Equal(["activity:1", "activity:2"], ids);
    }

    [SkippableFact]
    public async Task GetActivityIdsNeedingProcessing_SkipsRecentlyEnqueuedActivities()
    {
        await fixture.ResetAsync();
        await InsertActivityAsync("activity:1");
        await InsertActivityAsync("activity:2");
        await fixture.ExecuteAsync(
            """
            UPDATE activities
            SET processing_status = jsonb_set('{}'::jsonb, '{enqueuedAtUtc}', to_jsonb(NOW()::text))
            WHERE id = 'activity:2'
            """);

        var ids = await new BackendAdminQueries(fixture.DataSource)
            .GetActivityIdsNeedingProcessing(1, 1, 1, limit: 100);

        Assert.Equal(["activity:1"], ids);
    }

    [SkippableFact]
    public async Task MarkMethods_StampQueuedAtAndHideRowsFromPolls()
    {
        await fixture.ResetAsync();
        await InsertActivityAsync("activity:1");
        await InsertBoundaryAsync("admin:1");

        var queries = new BackendAdminQueries(fixture.DataSource);
        var queuedAt = DateTime.UtcNow;
        await queries.MarkActivitiesEnqueued(["activity:1"], queuedAt);
        await queries.MarkAdminBoundariesQueued(["admin:1"], queuedAt);

        Assert.Empty(await queries.GetActivityIdsNeedingProcessing(1, 1, 1, 100));
        Assert.Empty(await queries.GetStaleAdminBoundaryIds(1, null, 100));
    }

    [SkippableFact]
    public async Task GetStaleAdminBoundaryIds_WithAfterId_ContinuesKeysetPaging()
    {
        await fixture.ResetAsync();
        await InsertBoundaryAsync("admin:1");
        await InsertBoundaryAsync("admin:2");
        await InsertBoundaryAsync("admin:3");

        var queries = new BackendAdminQueries(fixture.DataSource);
        var firstPage = await queries.GetStaleAdminBoundaryIds(1, afterId: null, limit: 2);
        var secondPage = await queries.GetStaleAdminBoundaryIds(1, afterId: firstPage[^1], limit: 2);

        Assert.Equal(["admin:1", "admin:2"], firstPage);
        Assert.Equal(["admin:3"], secondPage);
    }

    [SkippableFact]
    public async Task GetActivityIdsWithMissingOsmHighwayId_FirstPageWithNullAfterId_ReturnsIdsWithoutPaths()
    {
        await fixture.ResetAsync();
        await InsertActivityAsync("activity:1");
        await InsertPathAsync("activity:2", "way/2");
        await InsertActivityAsync("activity:3");

        var ids = await new BackendAdminQueries(fixture.DataSource)
            .GetActivityIdsWithMissingOsmHighwayId(afterActivityId: null, limit: 100);

        Assert.Equal(["activity:1", "activity:3"], ids);
    }

    [SkippableFact]
    public async Task GetActivityIdsWithMissingOsmHighwayId_WithAfterId_ContinuesKeysetPaging()
    {
        await fixture.ResetAsync();
        await InsertActivityAsync("activity:1");
        await InsertActivityAsync("activity:2");
        await InsertActivityAsync("activity:3");

        var queries = new BackendAdminQueries(fixture.DataSource);
        var firstPage = await queries.GetActivityIdsWithMissingOsmHighwayId(null, limit: 2);
        var secondPage = await queries.GetActivityIdsWithMissingOsmHighwayId(firstPage[^1], limit: 2);

        Assert.Equal(["activity:1", "activity:2"], firstPage);
        Assert.Equal(["activity:3"], secondPage);
    }

    private async Task InsertActivityAsync(string id)
    {
        await fixture.ExecuteAsync("INSERT INTO users (id) VALUES ('user:1') ON CONFLICT DO NOTHING");
        await fixture.ExecuteAsync(
            """
            INSERT INTO activities (id, user_id, name, sport_type, start_date, start_date_local)
            VALUES (@id, 'user:1', 'Run', 'Run', NOW(), NOW())
            ON CONFLICT DO NOTHING
            """,
            ("id", id));
    }

    private Task SetStageEpochAsync(string activityId, string key, long epoch)
        => fixture.ExecuteAsync(
            """
            UPDATE activities
            SET processing_status = COALESCE(processing_status, '{}'::jsonb)
                || jsonb_build_object(@key, @epoch::bigint)
            WHERE id = @id
            """,
            ("id", activityId), ("key", key), ("epoch", epoch));

    private Task InsertBoundaryAsync(string id, long generation = 0)
        => fixture.ExecuteAsync(
            """
            INSERT INTO admin_boundaries
                (id, osm_id, osm_type, admin_level, name, geom, properties, metrics_generation)
            VALUES
                (@id, 1, 'relation', 4, 'Stale', ST_SetSRID(ST_GeomFromGeoJSON(@geom), 4326),
                 '{}'::jsonb, @generation)
            """,
            ("id", id), ("geom", Polygon), ("generation", generation));

    private async Task InsertActivityAsync(string id)
    {
        await fixture.ExecuteAsync("INSERT INTO users (id) VALUES ('user:1') ON CONFLICT DO NOTHING");
        await fixture.ExecuteAsync(
            """
            INSERT INTO activities (id, user_id, name, sport_type, start_date, start_date_local)
            VALUES (@id, 'user:1', 'Run', 'Run', NOW(), NOW())
            ON CONFLICT DO NOTHING
            """,
            ("id", id));
    }

    private async Task InsertPathAsync(string activityId, string osmHighwayId)
    {
        await InsertActivityAsync(activityId);
        await fixture.ExecuteAsync(
            """
            INSERT INTO activity_visited_tiles (activity_id, user_id, tile_x, tile_y, highway_ids)
            VALUES (@activity_id, 'user:1', 0, 0, ARRAY[@highway_id]::text[])
            """,
            ("activity_id", activityId), ("highway_id", osmHighwayId));
    }
}
