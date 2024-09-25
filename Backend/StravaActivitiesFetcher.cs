using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services.StravaClient;

namespace Backend
{
    public class StravaActivitiesFetcher(ILogger<StravaActivitiesFetcher> _logger, IHttpClientFactory httpClientFactory, ActivitiesApi _activitiesApi)
    {
        readonly HttpClient _apiClient = httpClientFactory.CreateClient("backendApiClient");

        [Function(nameof(StravaActivitiesFetcher))]
        public async Task<Outputs> Run([ServiceBusTrigger("activitiesfetchjobs", Connection = "ServicebusConnection")] ActivitiesFetchJob fetchJob)
        {
            var accessTokenResponse = await _apiClient.GetAsync($"{fetchJob.UserId}/accessToken");
            var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();

            var page = fetchJob.Page ?? 1;

            var (activites, hasMorePages) = await _activitiesApi.GetActivitiesByAthlete(accessToken, page, fetchJob.Before, fetchJob.After);

            var outputs = new Outputs()
            {
                WriteToActivities = activites.Select(ActivityMapper.MapSummaryActivity)
            };

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
            [CosmosDBOutput("%CosmosDb%", "%ActivitiesContainer%", Connection = "CosmosDBConnection")]
            public required IEnumerable<Activity> WriteToActivities { get; set; }
            [ServiceBusOutput("activitiesfetchjobs", Connection = "ServicebusConnection")]
            public ActivitiesFetchJob? NextPageJob { get; set; }
        }
    }
}
