using Microsoft.Extensions.Configuration;
using Stigar.Core.Jobs;

namespace Stigar.Core.Tests;

/// <summary>
/// Hangfire worker concurrency is configurable per host so deployments can tune it
/// without a rebuild. These tests pin the default and the config override.
/// </summary>
public sealed class HangfireInfrastructureTests
{
    [Fact]
    public void GetWorkerCount_WhenNotConfigured_ReturnsDefaultOfEight()
    {
        var configuration = new ConfigurationBuilder().Build();

        Assert.Equal(8, HangfireInfrastructure.GetWorkerCount(configuration));
    }

    [Fact]
    public void GetWorkerCount_ReadsConfiguredValue()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [HangfireInfrastructure.WorkerCountKey] = "12"
            })
            .Build();

        Assert.Equal(12, HangfireInfrastructure.GetWorkerCount(configuration));
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-3")]
    [InlineData("not-a-number")]
    public void GetWorkerCount_WhenValueIsNotPositive_FallsBackToDefault(string value)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [HangfireInfrastructure.WorkerCountKey] = value
            })
            .Build();

        Assert.Equal(8, HangfireInfrastructure.GetWorkerCount(configuration));
    }
}
