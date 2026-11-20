using Microsoft.AspNetCore.Mvc;
using System.Net;
using Stigar.Api.Http;
using Hangfire;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json.Nodes;
using Stigar.Core.Jobs;

namespace Stigar.Api.Endpoints.Backend;

/// <summary>Jobs produced by a single webhook event. Kept as a plain DTO so the
/// event-mapping rules can be tested without a Hangfire store.</summary>
internal sealed class WebhookJobs
{
    public ActivityFetchJob? ActivityFetchJob { get; set; }
    public ActivityDeleteJob? ActivityDeleteJob { get; set; }
    public AccountDeleteJob? AccountDeleteJob { get; set; }
}

/// <summary>
/// Strava webhook receiver. Previously used Functions output bindings to fan
/// out to three queues; it now enqueues strongly-typed Hangfire jobs directly.
/// </summary>
public class StravaWebhook(
    ILogger<StravaWebhook> _logger,
    IConfiguration _configuration,
    IBackgroundJobClient jobs)
{
    private const string Path = "/api/StravaWebhook";

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapMethods(Path, ["GET", "POST"],
            (HttpContext context, [FromServices] StravaWebhook endpoint) => endpoint.Handle(context));

    public async Task<IResult> Handle(HttpContext context)
    {
        if (HttpMethods.IsPost(context.Request.Method))
        {
            _logger.LogInformation("Webhook event received!");
            string requestBody;
            try
            {
                using var reader = new StreamReader(context.Request.Body);
                requestBody = await reader.ReadToEndAsync(context.RequestAborted);
            }
            catch (BadHttpRequestException ex)
            {
                _logger.LogWarning(ex, "Strava webhook request body could not be read.");
                return TypedResults.BadRequest();
            }

            var data = string.IsNullOrWhiteSpace(requestBody) ? null : JsonNode.Parse(requestBody);
            var outputs = new WebhookJobs();
            if (!TryApplyWebhookEvent(data, outputs))
                _logger.LogError("Unhandled webhook type");
            Enqueue(outputs);

            return TypedResults.Content("EVENT_RECEIVED", "text/plain");
        }

        if (HttpMethods.IsGet(context.Request.Method))
        {
            var verifyToken = _configuration.GetValue<string>("stravaVerifyToken");

            var mode = context.Request.Query["hub.mode"].ToString();
            var token = context.Request.Query["hub.verify_token"].ToString();
            var challenge = context.Request.Query["hub.challenge"].ToString();
            if (!string.IsNullOrEmpty(mode) && !string.IsNullOrEmpty(token)
                && mode == "subscribe" && token == verifyToken)
            {
                _logger.LogInformation("Webhook verified!");
                return TypedResults.Json(new { hub = new { challenge } });
            }
            return TypedResults.StatusCode(StatusCodes.Status403Forbidden);
        }

        return TypedResults.BadRequest();
    }

    internal static bool TryApplyWebhookEvent(JsonNode? data, WebhookJobs outputs)
    {
        var objectType = data?["object_type"]?.ToString();
        var aspectType = data?["aspect_type"]?.ToString();
        var activityId = data?["object_id"]?.ToString();
        var userId = data?["owner_id"]?.ToString();

        if (objectType == "activity" && (aspectType == "update" || aspectType == "create")
            && !string.IsNullOrEmpty(activityId) && !string.IsNullOrEmpty(userId))
        {
            outputs.ActivityFetchJob = new ActivityFetchJob { ActivityId = activityId, UserId = userId };
            return true;
        }

        if (objectType == "activity" && aspectType == "delete"
            && !string.IsNullOrEmpty(activityId) && !string.IsNullOrEmpty(userId))
        {
            outputs.ActivityDeleteJob = new ActivityDeleteJob { ActivityId = activityId, UserId = userId };
            return true;
        }

        if (objectType == "athlete" && aspectType == "update"
            && data?["updates"]?["authorized"]?.ToString() == "false"
            && !string.IsNullOrEmpty(userId))
        {
            outputs.AccountDeleteJob = new AccountDeleteJob { UserId = userId };
            return true;
        }

        return false;
    }

    private void Enqueue(WebhookJobs outputs)
    {
        if (outputs.ActivityFetchJob is not null)
            jobs.EnqueueActivityFetch(outputs.ActivityFetchJob);
        if (outputs.ActivityDeleteJob is not null)
            jobs.EnqueueActivityDelete(outputs.ActivityDeleteJob);
        if (outputs.AccountDeleteJob is not null)
            jobs.EnqueueAccountDelete(outputs.AccountDeleteJob);
    }
}
