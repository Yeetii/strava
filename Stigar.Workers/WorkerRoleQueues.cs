using Stigar.Core.Constants;

namespace Stigar.Workers;

/// <summary>
/// Maps WORKER_ROLE to the Hangfire queues a worker host consumes. The schedule
/// role is the sole consumer of backend-polling and recurring-jobs; the heavy
/// role consumes only the typed job queues.
/// </summary>
public static class WorkerRoleQueues
{
    public static string[] ForRole(string role, IEnumerable<string> typedQueues)
    {
        ArgumentNullException.ThrowIfNull(role);
        ArgumentNullException.ThrowIfNull(typedQueues);

        return string.Equals(role, "heavy", StringComparison.OrdinalIgnoreCase)
            ? typedQueues.ToArray()
            : [AppConfig.BackendPollingQueueName, AppConfig.RecurringJobsQueueName];
    }
}
