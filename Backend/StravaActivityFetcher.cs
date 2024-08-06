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
        public int? Before { get; set; }
        [JsonPropertyName("after")]
        public int? After { get; set; }

    }

    public class StravaActivityFetcher
    {
        private readonly ILogger<StravaActivityFetcher> _logger;

        public StravaActivityFetcher(ILogger<StravaActivityFetcher> logger)
        {
            _logger = logger;
        }

        [Function(nameof(StravaActivityFetcher))]
        public async Task Run([ServiceBusTrigger("activityFetchJobs", Connection = "ServicebusConnection")] ActivityFetchJob fetchJob,
        [CosmosDBInput(
            databaseName: "%CosmosDb%",
            containerName: "%UsersContainer%",
            Connection  = "CosmosDBConnection",
            Id = "{userId}",
            PartitionKey = "{userId}")] User user)
        {
            // Get access token from userId

            // Fetch activity from access token and activity id
            var activites = await ActivitiesAPI.GetStravaModel(user.AccessToken);
            _logger.LogInformation("", activites.Count());

            // Save activity to cosmos
        }
    }
}
