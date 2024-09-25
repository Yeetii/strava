using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services.StravaClient;

namespace Backend
{
    public class ActivityFetchJob
    {
        public required string UserId { get; set; }
        public required string ActivityId { get; set; }
    }

    public class StravaActivityFetcher(ILogger<StravaActivityFetcher> _logger, IHttpClientFactory httpClientFactory, ActivitiesApi _activitiesApi)
    {
        readonly HttpClient _apiClient = httpClientFactory.CreateClient("backendApiClient");
        [CosmosDBOutput("%CosmosDb%", "%ActivitiesContainer%", Connection = "CosmosDBConnection")]
        [Function(nameof(StravaActivityFetcher))]
        public async Task<Activity?> Run([ServiceBusTrigger("activityFetchJobs", Connection = "ServicebusConnection")] ActivityFetchJob fetchJob)
        {
            Thread.Sleep(1000);
            var accessTokenResponse = await _apiClient.GetAsync($"{fetchJob.UserId}/accessToken");
            var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();

            var activity = await _activitiesApi.GetActivity(accessToken, fetchJob.ActivityId);
            return ActivityMapper.MapDetailedActivity(activity);
        }

        public class Outputs
        {
            [CosmosDBOutput("%CosmosDb%", "%ActivitiesContainer%", Connection = "CosmosDBConnection")]
            public object WriteToActivities { get; set; }
        }
    }
}
