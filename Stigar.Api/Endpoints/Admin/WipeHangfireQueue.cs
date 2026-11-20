using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Stigar.Api.Http;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;
using System.Threading;
using System.Threading.Tasks;

namespace Stigar.Api.Endpoints.Admin;

public record WipeQueueResponse(string Queue, int Deleted, long Remaining);

/// <summary>
/// Deletes a bounded batch of enqueued jobs in a single Hangfire queue. The caller repeats
/// the request until Remaining is zero; this keeps each HTTP request short enough to finish
/// even on queues with hundreds of thousands of jobs. Scheduled and failed jobs are global
/// in Hangfire and are deliberately not touched.
/// </summary>
public class WipeHangfireQueue(
    IConfiguration configuration,
    ILogger<WipeHangfireQueue> logger)
{
    private const int DefaultBatchSize = 2000;
    private const int MaxBatchSize = 5000;
    private const int DeleteConcurrency = 16;

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapDelete("/api/manage/queues/{queueName}",
            (HttpContext context, [FromServices] WipeHangfireQueue endpoint, string queueName) =>
                endpoint.Handle(context, queueName));

    public IResult Handle(HttpContext context, string queueName)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var queue = JobEnqueueExtensions.NormalizeQueue(queueName);
        var rawLimit = context.Request.Query["limit"].ToString();
        var limit = int.TryParse(rawLimit, out var parsedLimit) && parsedLimit > 0
            ? Math.Min(parsedLimit, MaxBatchSize)
            : DefaultBatchSize;

        try
        {
            var monitoring = JobStorage.Current?.GetMonitoringApi();
            if (monitoring is null)
                return TypedResults.StatusCode(StatusCodes.Status503ServiceUnavailable);

            var knownQueues = QueueConfig.AllQueues
                .Select(JobEnqueueExtensions.NormalizeQueue)
                .Concat(monitoring.Queues().Select(q => JobEnqueueExtensions.NormalizeQueue(q.Name)))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            if (!knownQueues.Contains(queue))
                return TypedResults.NotFound();

            var jobs = monitoring.EnqueuedJobs(queue, 0, limit);
            if (jobs.Count == 0)
                return TypedResults.Ok(
                    new WipeQueueResponse(queue, 0, monitoring.EnqueuedCount(queue)));

            var deleted = 0;
            Parallel.ForEach(
                jobs,
                new ParallelOptions { MaxDegreeOfParallelism = DeleteConcurrency },
                job =>
                {
                    if (BackgroundJob.Delete(job.Key))
                        Interlocked.Increment(ref deleted);
                });

            var remaining = monitoring.EnqueuedCount(queue);
            logger.LogInformation(
                "Wiped {Deleted} enqueued jobs from queue {Queue}, {Remaining} left",
                deleted, queue, remaining);
            return TypedResults.Ok(new WipeQueueResponse(queue, deleted, remaining));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to wipe enqueued jobs from queue {Queue}", queue);
            return TypedResults.StatusCode(StatusCodes.Status500InternalServerError);
        }
    }
}
