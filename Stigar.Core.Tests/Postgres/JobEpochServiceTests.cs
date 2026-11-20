using Stigar.Core.Jobs;
using Stigar.Core.Services.Postgres.Admin;
using Xunit;

namespace Stigar.Core.Tests.Postgres;

[Collection(PostgisCollection.Name)]
public sealed class JobEpochServiceTests(PostgisFixture fixture)
{
    [SkippableFact]
    public async Task GetAllAsync_ReturnsEverySeededJobTypeAtEpochOne()
    {
        await fixture.ResetAsync();

        var epochs = await new JobEpochService(fixture.DataSource).GetAllAsync();

        Assert.Equal(JobEpochKeys.All.OrderBy(key => key, StringComparer.Ordinal),
            epochs.Select(entry => entry.JobType));
        Assert.All(epochs, entry => Assert.Equal(1, entry.Epoch));
    }

    [SkippableFact]
    public async Task BumpAsync_IncrementsOnlyTheRequestedJobType()
    {
        await fixture.ResetAsync();
        var service = new JobEpochService(fixture.DataSource);

        var bumped = await service.BumpAsync(JobEpochType.ActivityVisitedPaths);

        Assert.Equal(2, bumped);
        Assert.Equal(2, await service.GetAsync(JobEpochType.ActivityVisitedPaths));
        Assert.Equal(1, await service.GetAsync(JobEpochType.ActivitySummits));
        Assert.Equal(1, await service.GetAsync(JobEpochType.AdminBoundaryMetrics));
    }

    [SkippableFact]
    public async Task GetAsync_ResolvesMissingRowToOne()
    {
        await fixture.ResetAsync();
        await fixture.ExecuteAsync("DELETE FROM job_epochs WHERE job_type = 'activity.summits'");

        Assert.Equal(1, await new JobEpochService(fixture.DataSource).GetAsync(JobEpochType.ActivitySummits));
    }
}
