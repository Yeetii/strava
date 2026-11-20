using Stigar.Core.Services.Postgres;
using Xunit;

namespace Stigar.Core.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class UserSyncItemRepositoryTests(PostgisFixture fixture)
{
    [SkippableFact]
    public async Task GetLatestUpdatedAtByUsers_ReturnsMaxPerUser()
    {
        await fixture.ResetAsync();
        await InsertUserAsync("user:1");
        await InsertUserAsync("user:2");
        await InsertItemAsync("user:1", "setting", "libraryData", 100);
        await InsertItemAsync("user:1", "file", "route", 300);
        await InsertItemAsync("user:2", "setting", "libraryData", 200);

        var result = await new UserSyncItemRepository(fixture.DataSource)
            .GetLatestUpdatedAtByUsers();

        Assert.Equal(300, result["user:1"]);
        Assert.Equal(200, result["user:2"]);
        Assert.DoesNotContain("user:missing", result.Keys);
    }

    [SkippableFact]
    public async Task GetLatestUpdatedAt_ReturnsUserMaximumOrNull()
    {
        await fixture.ResetAsync();
        await InsertUserAsync("user:1");
        await InsertItemAsync("user:1", "setting", "libraryData", 100);
        await InsertItemAsync("user:1", "file", "route", 300);
        await InsertUserAsync("user:2");

        var repository = new UserSyncItemRepository(fixture.DataSource);

        Assert.Equal(300, await repository.GetLatestUpdatedAt("user:1"));
        Assert.Null(await repository.GetLatestUpdatedAt("user:2"));
    }

    private Task InsertUserAsync(string userId) =>
        fixture.ExecuteAsync("INSERT INTO users (id) VALUES (@id)", ("id", userId));

    private Task InsertItemAsync(string userId, string category, string key, long updatedAt) =>
        fixture.ExecuteAsync(
            """
            INSERT INTO user_sync_items (id, user_id, category, key, updated_at)
            VALUES (@id, @user_id, @category, @key, @updated_at)
            """,
            ("id", $"userSync:{category}:{userId}:{key}"),
            ("user_id", userId),
            ("category", category),
            ("key", key),
            ("updated_at", updatedAt));
}
