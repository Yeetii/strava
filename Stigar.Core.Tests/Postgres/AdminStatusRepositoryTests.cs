using Stigar.Core.Services.Postgres.Admin;
using Xunit;

namespace Stigar.Core.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class AdminStatusRepositoryTests(PostgisFixture fixture)
{
    [SkippableFact]
    public async Task GetTableStatsAsync_ReturnsRowCountAndTableSize()
    {
        await fixture.ResetAsync();
        await fixture.ExecuteAsync("INSERT INTO users (id) VALUES ('user:1'), ('user:2')");

        var stats = await new AdminStatusRepository(fixture.DataSource)
            .GetTableStatsAsync("users");

        Assert.Equal(2, stats.RowCount);
        Assert.True(stats.SizeBytes > 0);
    }

    [Fact]
    public async Task GetTableStatsAsync_RejectsUnknownTable()
    {
        var repository = new AdminStatusRepository(fixture.DataSource);

        await Assert.ThrowsAsync<ArgumentException>(
            () => repository.GetTableStatsAsync("users; DROP TABLE users"));
    }
}
