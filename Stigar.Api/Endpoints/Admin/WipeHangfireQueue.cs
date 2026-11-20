using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using Stigar.Api.Http;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;

namespace Stigar.Api.Endpoints.Admin;

public record WipeQueueResponse(string Queue, bool Emptied);

/// <summary>
/// Empties a Hangfire queue by deleting its Redis list key. This is O(1) regardless of
/// queue length and skips Hangfire's per-job state changes. Scheduled and failed jobs are
/// global in Hangfire and are deliberately not touched.
/// </summary>
public class WipeHangfireQueue(
    IConfiguration configuration,
    ILogger<WipeHangfireQueue> logger,
    IDatabase redis)
{
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

            var prefix = HangfireInfrastructure.GetRedisKeyPrefix(configuration);
            redis.KeyDelete($"{prefix}queue:{queue}");

            logger.LogInformation("Emptied Hangfire queue {Queue}", queue);
            return TypedResults.Ok(new WipeQueueResponse(queue, true));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to empty Hangfire queue {Queue}", queue);
            return TypedResults.StatusCode(StatusCodes.Status500InternalServerError);
        }
    }
}
