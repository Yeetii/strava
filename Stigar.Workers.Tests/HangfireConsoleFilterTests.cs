using Hangfire.Common;
using Hangfire.Server;
using Stigar.Core.Jobs;

namespace Stigar.Workers.Tests;

public class HangfireConsoleFilterTests
{
    [Fact]
    public void AddConsoleServerFilter_RegistersExactlyOneServerFilter()
    {
        var filters = new JobFilterCollection();

        HangfireInfrastructure.AddConsoleServerFilter(filters);
        HangfireInfrastructure.AddConsoleServerFilter(filters);

        var consoleFilters = filters.Select(f => f.Instance).OfType<JobConsoleServerFilterAttribute>().ToList();
        Assert.Single(consoleFilters);
        Assert.IsAssignableFrom<IServerFilter>(consoleFilters[0]);
    }
}
