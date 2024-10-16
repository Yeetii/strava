using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;

namespace Backend
{
    public class StravaActivitiesFetcher(ILogger<StravaActivitiesFetcher> _logger, IHttpClientFactory httpClientFactory, ActivitiesApi _activitiesApi, CollectionClient<Activity> _activitiesCollection)
    {
        readonly HttpClient _apiClient = httpClientFactory.CreateClient("backendApiClient");
        [ServiceBusOutput("activitiesfetchjobs", Connection = "ServicebusConnection")]
        [Function(nameof(StravaActivitiesFetcher))]
        public async Task<ActivitiesFetchJob?> Run([ServiceBusTrigger("activitiesfetchjobs", Connection = "ServicebusConnection")] ActivitiesFetchJob fetchJob)
        {
            var accessTokenResponse = await _apiClient.GetAsync($"{fetchJob.UserId}/accessToken");
            var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();

            var page = fetchJob.Page ?? 1;

            var (activites, hasMorePages) = await _activitiesApi.GetActivitiesByAthlete(accessToken, page, fetchJob.Before, fetchJob.After);

            _logger.LogInformation("Fetched {amount} activities", activites?.Count() ?? 0);
            if (activites != null)
                await _activitiesCollection.BulkUpsert(activites.Select(ActivityMapper.MapSummaryActivity));
                
            if (hasMorePages)
            {
                fetchJob.Page = ++page;
                return fetchJob;
            }
            return default;
        }
    }
}
