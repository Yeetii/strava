using Microsoft.Extensions.Logging;
using Stigar.Core.Jobs;

namespace Stigar.Workers;

/// <summary>
/// Executes a native recurring job by its stable id. Hangfire targets the shared
/// <see cref="IRecurringJobDispatcher"/> interface so every host reading the store
/// can deserialize the job, while the concrete IRecurringJob implementations stay
/// in the host that actually runs them.
/// </summary>
public sealed class RecurringJobDispatcher(
    IEnumerable<IRecurringJob> jobs,
    ILogger<RecurringJobDispatcher> logger) : IRecurringJobDispatcher
{
    public async Task RunAsync(string jobId)
    {
        var job = jobs.FirstOrDefault(candidate => candidate.JobId == jobId)
            ?? throw new InvalidOperationException($"Unknown recurring job '{jobId}'.");

        logger.LogInformation("Executing recurring job {JobId}", jobId);
        await job.ExecuteAsync(CancellationToken.None);
    }
}
