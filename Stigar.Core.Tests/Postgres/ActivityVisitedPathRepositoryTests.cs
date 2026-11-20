using Stigar.Core.Services.Postgres.Spatial;
using Xunit;

namespace Stigar.Core.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class ActivityVisitedPathRepositoryTests(PostgisFixture fixture)
{
    [SkippableFact]
    public async Task GetSummaryByUserAndTiles_AggregatesHighwaysAcrossActivitiesAndFiltersTiles()
    {
        await fixture.ResetAsync();
        var first = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);
        var last = new DateTime(2026, 1, 2, 10, 0, 0, DateTimeKind.Utc);
        await InsertActivityAsync("activity:1", first);
        await InsertActivityAsync("activity:2", last);
        await InsertTileAsync("activity:1", 1, 2, ["way/10", "way/20"]);
        await InsertTileAsync("activity:2", 1, 2, ["way/10", "way/30"]);
        await InsertTileAsync("activity:2", 5, 6, ["way/40"]);

        var repository = new ActivityVisitedPathRepository(fixture.DataSource);
        var summaries = await repository.GetSummaryByUserAndTiles("user:1", [(1, 2)]);

        Assert.Equal(3, summaries.Count);
        Assert.Equal("way/10", summaries[0].OsmHighwayId);
        Assert.Equal(2, summaries[0].TimesVisited);
        Assert.Equal(first, summaries[0].FirstDate);
        Assert.Equal(last, summaries[0].LastDate);
        Assert.DoesNotContain(summaries, s => s.OsmHighwayId == "way/40");
        Assert.Equal(5, await repository.CountByUser("user:1"));
    }

    [SkippableFact]
    public async Task AddRange_GroupsPathsByActivityAndTile()
    {
        await fixture.ResetAsync();
        await InsertActivityAsync("activity:1", DateTime.UtcNow);

        var repository = new ActivityVisitedPathRepository(fixture.DataSource);
        await repository.AddRange([
            new VisitedPathInput("activity:1", "user:1", "way/20", 1, 2),
            new VisitedPathInput("activity:1", "user:1", "way/10", 1, 2),
            new VisitedPathInput("activity:1", "user:1", "way/30", 3, 4),
        ]);

        var links = await repository.GetByActivity("activity:1");
        Assert.Equal(2, links.Count);
        Assert.Equal(["way/10", "way/20"], links.Single(l => l.TileX == 1 && l.TileY == 2).HighwayIds);
        Assert.Equal(3, await repository.CountByUser("user:1"));
    }

    private async Task InsertActivityAsync(string id, DateTime startDateLocal)
    {
        await fixture.ExecuteAsync("INSERT INTO users (id) VALUES ('user:1') ON CONFLICT DO NOTHING");
        await fixture.ExecuteAsync(
            """
            INSERT INTO activities (id, user_id, name, sport_type, start_date, start_date_local)
            VALUES (@id, 'user:1', 'Run', 'Run', @date, @date)
            """,
            ("id", id), ("date", startDateLocal));
    }

    private async Task InsertTileAsync(string activityId, int tileX, int tileY, string[] highwayIds)
        => await fixture.ExecuteAsync(
            """
            INSERT INTO activity_visited_tiles (activity_id, user_id, tile_x, tile_y, highway_ids)
            VALUES (@activity_id, 'user:1', @tile_x, @tile_y, @highway_ids)
            """,
            ("activity_id", activityId), ("tile_x", tileX), ("tile_y", tileY), ("highway_ids", highwayIds));
}
