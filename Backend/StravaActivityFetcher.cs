using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Azure.Messaging.ServiceBus;
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
        [JsonPropertyName("activityId")]
        public string? ActivityId { get; set; }
        [JsonPropertyName("userId")]
        public string? UserId { get; set; }
    }

    public class StravaActivityFetcher
    {
        private readonly ILogger<StravaActivityFetcher> _logger;

        public StravaActivityFetcher(ILogger<StravaActivityFetcher> logger)
        {
            _logger = logger;
        }

        [Function(nameof(StravaActivityFetcher))]
        public void Run([ServiceBusTrigger("activityFetchJobs", Connection = "ServicebusConnection")] ActivityFetchJob fetchJob,
        [CosmosDBInput(
            databaseName: "%CosmosDb%",
            containerName: "%UsersContainer%",
            Connection  = "CosmosDBConnection",
            Id = "{userId}",
            PartitionKey = "{userId}")] User user)
        {
            // Get access token from userId

            // Fetch activity from access token and activity id

            // Save activity to cosmos
        }
    }
}
