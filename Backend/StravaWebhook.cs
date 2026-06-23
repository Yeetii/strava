using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Shared.Models;


namespace Backend
{
    public class OutputBindings
    {
        [HttpResult]
        public required IActionResult Response { get; set;}
        [ServiceBusOutput(Shared.Constants.ServiceBusConfig.ActivityFetchJobs, Connection = "ServiceBusConnection")]
        public ActivityFetchJob? ActivityFetchJob { get; set; }
        [ServiceBusOutput(Shared.Constants.ServiceBusConfig.ActivityDeleteJobs, Connection = "ServiceBusConnection")]
        public ActivityDeleteJob? ActivityDeleteJob { get; set; }
        [ServiceBusOutput(Shared.Constants.ServiceBusConfig.AccountDeleteJobs, Connection = "ServiceBusConnection")]
        public AccountDeleteJob? AccountDeleteJob { get; set; }
    }
    public class StravaWebhook(ILogger<StravaWebhook> _logger, IConfiguration _configuration)
    {
        [Function(nameof(StravaWebhook))]
        public async Task<OutputBindings> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req)
        {
            var outputs = new OutputBindings(){Response = new StatusCodeResult((int)HttpStatusCode.OK)}; 
            // Where webhook updates from strava are received
            if (HttpMethods.IsPost(req.Method))
            {
                _logger.LogInformation("Webhook event received!");
                string requestBody;
                try
                {
                    requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                }
                catch (BadHttpRequestException ex)
                {
                    _logger.LogWarning(ex, "Strava webhook request body could not be read.");
                    outputs.Response = new BadRequestResult();
                    return outputs;
                }

                var data = JsonConvert.DeserializeObject<JObject>(requestBody);
                if (!TryApplyWebhookEvent(data, outputs))
                    _logger.LogError("Unhandled webhook type");
                outputs.Response = new ContentResult { Content = "EVENT_RECEIVED", StatusCode = (int)HttpStatusCode.OK };
                return outputs;
            }
            // Only used to create new webhook subscription at Strava
            else if (HttpMethods.IsGet(req.Method))
            {
                string? verifyToken = _configuration.GetValue<string>("stravaVerifyToken");

                string? mode = req.Query["hub.mode"];
                string? token = req.Query["hub.verify_token"];
                string? challenge = req.Query["hub.challenge"];
                if (!string.IsNullOrEmpty(mode) && !string.IsNullOrEmpty(token) 
                    && mode == "subscribe" && token == verifyToken)
                {
                    _logger.LogInformation("Webhook verified!");
                    outputs.Response = new ContentResult
                    {
                        Content = "{\"hub.challenge\": \"" + challenge + "\"}",
                        ContentType = "application/json",
                        StatusCode = (int)HttpStatusCode.OK
                    };
                    return outputs;
                }
                outputs.Response = new StatusCodeResult((int)HttpStatusCode.Forbidden);
                return outputs;
            }
            outputs.Response = new BadRequestResult();
            return outputs;
        }

        internal static bool TryApplyWebhookEvent(JObject? data, OutputBindings outputs)
        {
            var objectType = data?["object_type"]?.ToString();
            var aspectType = data?["aspect_type"]?.ToString();
            var activityId = data?["object_id"]?.ToString();
            var userId = data?["owner_id"]?.ToString();

            if (objectType == "activity" && (aspectType == "update" || aspectType == "create")
                && !string.IsNullOrEmpty(activityId) && !string.IsNullOrEmpty(userId))
            {
                outputs.ActivityFetchJob = new ActivityFetchJob{ActivityId = activityId, UserId = userId};
                return true;
            }

            if (objectType == "activity" && aspectType == "delete"
                && !string.IsNullOrEmpty(activityId) && !string.IsNullOrEmpty(userId))
            {
                outputs.ActivityDeleteJob = new ActivityDeleteJob{ActivityId = activityId, UserId = userId};
                return true;
            }

            if (objectType == "athlete" && aspectType == "update"
                && data?["updates"]?["authorized"]?.ToString() == "false"
                && !string.IsNullOrEmpty(userId))
            {
                outputs.AccountDeleteJob = new AccountDeleteJob{UserId = userId};
                return true;
            }

            return false;
        }
    }
}
