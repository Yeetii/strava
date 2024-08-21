using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services.StravaClient;

namespace Backend
{
    public class StravaActivityFetcher(ILogger<StravaActivityFetcher> _logger, IHttpClientFactory httpClientFactory)
    {
        readonly HttpClient _apiClient = httpClientFactory.CreateClient("apiClient");

        [Function(nameof(StravaActivityFetcher))]
        public async Task<Outputs> Run([ServiceBusTrigger("activityFetchJobs", Connection = "ServicebusConnection")] ActivityFetchJob fetchJob)
        {
            var tokenFunc = new Uri($"http://localhost:7072/api/{fetchJob.UserId}/accessToken");
            var accessTokenResponse = await _apiClient.GetAsync(tokenFunc);
            var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();
            
            var page = fetchJob.Page ?? 1;

            var (activites, hasMorePages) = await ActivitiesAPI.GetStravaModel(accessToken, page, fetchJob.Before, fetchJob.After);

            var outputs = new Outputs();

            if (hasMorePages)
            {
                fetchJob.Page = ++page;
                outputs.NextPageJob = fetchJob;
            }
            _logger.LogInformation("Fetched {amount} activities", activites?.Count() ?? 0);
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
