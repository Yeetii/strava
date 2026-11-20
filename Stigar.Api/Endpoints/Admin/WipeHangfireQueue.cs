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

namespace Stigar.Api.Endpoints.Admin;

public record WipeQueueResponse(string Queue, int Deleted);

/// <summary>
/// Deletes every enqueued job in a single Hangfire queue. Scheduled and failed jobs are
/// global in Hangfire and are deliberately not touched by this endpoint.
/// </summary>
public class WipeHangfireQueue(
    IConfiguration configuration,
    ILogger<WipeHangfireQueue> logger)
{
    private const int BatchSize = 500;

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapDelete("/api/manage/queues/{queueName}",
            (HttpContext context, [FromServices] WipeHangfireQueue endpoint, string queueName) =>
                endpoint.Handle(context, queueName));

    public IResult Handle(HttpContext context, string queueName)
    {
        if (!context.IsAdminAuthorized(configuration))
            return TypedResults.Unauthorized();

        var queue = JobEnqueueExtensions.NormalizeQueue(queueName);

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

            var deleted = 0;
            while (true)
            {
                var jobs = monitoring.EnqueuedJobs(queue, 0, BatchSize);
                if (jobs.Count == 0)
                    break;

                var deletedThisPass = 0;
                foreach (var job in jobs)
                {
                    if (BackgroundJob.Delete(job.Key))
                        deletedThisPass++;
                }

                if (deletedThisPass == 0)
                    break;

                deleted += deletedThisPass;
            }

            logger.LogInformation("Wiped {Deleted} enqueued jobs from queue {Queue}", deleted, queue);
            return TypedResults.Ok(new WipeQueueResponse(queue, deleted));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to wipe Hangfire queue {Queue}", queue);
            return TypedResults.StatusCode(StatusCodes.Status500InternalServerError);
        }
    }
}
