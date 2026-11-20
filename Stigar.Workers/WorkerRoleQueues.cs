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

        return IsHeavy(role)
            ? typedQueues.ToArray()
            : [AppConfig.BackendPollingQueueName, AppConfig.RecurringJobsQueueName];
    }

    /// <summary>
    /// The schedule role is the sole consumer of backend-polling and
    /// recurring-jobs; it runs one worker so those queues are processed one job
    /// at a time. The heavy role keeps the configured (or default) concurrency
    /// for the typed queues.
    /// </summary>
    public static int WorkerCountForRole(string role, int configuredWorkerCount) =>
        IsHeavy(role) ? configuredWorkerCount : 1;

    private static bool IsHeavy(string role) =>
        string.Equals(role, "heavy", StringComparison.OrdinalIgnoreCase);
}
