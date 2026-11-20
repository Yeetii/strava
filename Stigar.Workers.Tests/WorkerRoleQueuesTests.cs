using Stigar.Core.Constants;
using Stigar.Core.Jobs;

namespace Stigar.Workers.Tests;

/// <summary>
/// Pins the queue split between the two worker roles: only schedule consumes
/// backend-polling and recurring-jobs, and heavy never consumes either of them.
/// </summary>
public class WorkerRoleQueuesTests
{
    [Theory]
    [InlineData("schedule")]
    [InlineData("SCHEDULE")]
    [InlineData("")]
    public void ScheduleRoleConsumesOnlyTheSharedPollingAndRecurringQueues(string role)
    {
        var queues = WorkerRoleQueues.ForRole(role, QueueConfig.AllQueues);

        Assert.Equal(
            new[] { AppConfig.BackendPollingQueueName, AppConfig.RecurringJobsQueueName },
            queues);
    }

    [Theory]
    [InlineData("heavy")]
    [InlineData("HEAVY")]
    public void HeavyRoleConsumesOnlyTypedJobQueues(string role)
    {
        var typedQueues = QueueConfig.AllQueues
            .Select(JobEnqueueExtensions.NormalizeQueue)
            .Distinct()
            .ToArray();

        var queues = WorkerRoleQueues.ForRole(role, typedQueues);

        Assert.Equal(typedQueues, queues);
        Assert.DoesNotContain(AppConfig.BackendPollingQueueName, queues);
        Assert.DoesNotContain(AppConfig.RecurringJobsQueueName, queues);
    }

    [Theory]
    [InlineData("schedule")]
    [InlineData("SCHEDULE")]
    [InlineData("")]
    public void ScheduleRoleUsesOneWorkerForPollingAndRecurringQueues(string role)
    {
        Assert.Equal(1, WorkerRoleQueues.WorkerCountForRole(role, 8));
    }

    [Theory]
    [InlineData("heavy")]
    [InlineData("HEAVY")]
    public void HeavyRoleKeepsConfiguredWorkerCount(string role)
    {
        Assert.Equal(12, WorkerRoleQueues.WorkerCountForRole(role, 12));
    }
}
