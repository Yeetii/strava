using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;


namespace Backend
{
    public class OutputBindings
    {
        [HttpResult]
        public required HttpResponseData Response { get; set;}
        [ServiceBusOutput("activityFetchJobs", Connection = "ServicebusConnection")]
        public ActivityFetchJob? ActivityFetchJob { get; set; }
    }
    public class StravaWebhook(ILogger<StravaWebhook> _logger, IConfiguration _configuration)
    {
        [Function(nameof(StravaWebhook))]
        public async Task<OutputBindings> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequestData req)
        {
            var outputs = new OutputBindings(){Response = req.CreateResponse()}; 
            // Where webhook updates from strava are received
            if (req.Method == "POST")
            {
                _logger.LogInformation("Webhook event received!");
                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic? data = JsonConvert.DeserializeObject(requestBody);
                if (data?.object_type == "activity" && (data?.aspect_type == "update" || data?.aspect_type == "create"))
                {
                    outputs.ActivityFetchJob = new ActivityFetchJob{ActivityId = data.object_id, UserId = data.owner_id};
                }
                else
                {
                    _logger.LogError("Unhandled webhook type");
                }
                outputs.Response.StatusCode = System.Net.HttpStatusCode.OK;
                await outputs.Response.WriteStringAsync("EVENT_RECEIVED");
                return outputs;
            }
            // Only used to create new webhook subscription at Strava
            else if (req.Method == "GET")
            {
                string verifyToken = _configuration.GetValue<string>("stravaVerifyToken");

                string? mode = req.Query["hub.mode"];
                string? token = req.Query["hub.verify_token"];
                string? challenge = req.Query["hub.challenge"];
                if (!string.IsNullOrEmpty(mode) && !string.IsNullOrEmpty(token) 
                    && mode == "subscribe" && token == verifyToken)
                {
                    _logger.LogInformation("Webhook verified!");
                    outputs.Response.StatusCode = System.Net.HttpStatusCode.OK;
                    await outputs.Response.WriteStringAsync("{\"hub.challenge\": \"" + challenge + "\"}");
                    return outputs;
                }
                outputs.Response.StatusCode = System.Net.HttpStatusCode.Forbidden;
                return outputs;
            }
            outputs.Response.StatusCode = System.Net.HttpStatusCode.BadRequest;
            return outputs;
        }
    }
}