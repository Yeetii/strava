using System.Text.Json.Serialization;
using Backend.StravaClient;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Backend
{
    public class User
    {
        [JsonPropertyName("id")]
        public required string Id { get; set; }
        [JsonPropertyName("userName")]
        public string? UserName { get; set; }
        [JsonPropertyName("refreshToken")]
        public string? RefreshToken { get; set; }
        [JsonPropertyName("accessToken")]
        public string? AccessToken { get; set; }
        [JsonPropertyName("tokenExpiresAt")]
        public long TokenExpiresAt { get; set; }
    }

    public class ActivityFetchJob
    {
        [JsonPropertyName("userId")]
        public string? UserId { get; set; }
        [JsonPropertyName("page")]
        public int? Page { get; set;}
        [JsonPropertyName("before")]
        public DateTime? Before { get; set; }
        [JsonPropertyName("after")]
        public DateTime? After { get; set; }

    }

    public class StravaActivityFetcher
    {
        private readonly ILogger<StravaActivityFetcher> _logger;

        public StravaActivityFetcher(ILogger<StravaActivityFetcher> logger)
        {
            _logger = logger;
        }

        [Function(nameof(StravaActivityFetcher))]
        public async Task<Outputs> Run([ServiceBusTrigger("activityFetchJobs", Connection = "ServicebusConnection")] ActivityFetchJob fetchJob)
        {
            var httpClient = new HttpClient();
            var tokenFunc = new Uri($"http://localhost:7072/api/{fetchJob.UserId}/accessToken");
            var accessTokenResponse = await httpClient.GetAsync(tokenFunc);
            var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();
            
            var page = fetchJob.Page ?? 1;

            // Fetch activity from access token and activity id
            var (activites, hasMorePages) = await ActivitiesAPI.GetStravaModel(accessToken, page, fetchJob.Before, fetchJob.After);

            var outputs = new Outputs();

            if (hasMorePages)
            {
                fetchJob.Page = ++page;
                outputs.NextPageJob = fetchJob;
            }
            _logger.LogInformation("Dessaaa {counts}", activites.Count());
            outputs.WriteToActivities = activites.Select(ActivityMapper.MapSummaryActivity);
            return outputs;
        }

        public class Outputs
        {
            [CosmosDBOutput("%CosmosDb%", "%ActivitiesContainer%", Connection = "CosmosDBConnection", CreateIfNotExists = true, PartitionKey = "/id")]
            public object WriteToActivities { get; set;}
            [ServiceBusOutput("activityFetchJobs", Connection = "ServicebusConnection")]
            public ActivityFetchJob? NextPageJob { get; set; }
        }
    }
}
