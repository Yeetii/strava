using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using System.Text.Json;

namespace Backend
{
    public class StravaActivitiesFetcher(ILogger<StravaActivitiesFetcher> _logger, IHttpClientFactory httpClientFactory, ActivitiesApi _activitiesApi, CollectionClient<Activity> _activitiesCollection, ServiceBusClient serviceBusClient)
    {
        private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
        readonly HttpClient _apiClient = httpClientFactory.CreateClient("backendApiClient");
        readonly ServiceBusSender _sbSender = serviceBusClient.CreateSender(Shared.Constants.ServiceBusConfig.ActivitiesFetchJobs);

        [Function(nameof(StravaActivitiesFetcher))]
        public async Task Run(
            [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.ActivitiesFetchJobs, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
            ServiceBusMessageActions actions,
            CancellationToken cancellationToken)
        {
            var fetchJob = message.Body.ToObjectFromJson<ActivitiesFetchJob>();
            try
            {
                var accessTokenResponse = await _apiClient.GetAsync($"{fetchJob.UserId}/accessToken");
                if (!accessTokenResponse.IsSuccessStatusCode)
                {
                    var responseBody = await accessTokenResponse.Content.ReadAsStringAsync(cancellationToken);
                    throw new InvalidOperationException(
                        $"Failed to get access token for user {fetchJob.UserId}: {(int)accessTokenResponse.StatusCode} {accessTokenResponse.ReasonPhrase}. Response body: {responseBody}");
                }

                var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();

                var page = fetchJob.Page ?? 1;

                var (activites, hasMorePages) = await _activitiesApi.GetActivitiesByAthlete(accessToken, page, fetchJob.Before, fetchJob.After);
                if (activites is null)
                {
                    _logger.LogInformation("Strava athlete {UserId} not found (404); completing message.", fetchJob.UserId);
                    await actions.CompleteMessageAsync(message, cancellationToken);
                    return;
                }

                _logger.LogInformation("Fetched {amount} activities", activites.Count());
                await _activitiesCollection.BulkUpsert(activites.Select(ActivityMapper.MapSummaryActivity));

                if (hasMorePages)
                {
                    fetchJob.Page = ++page;
                    await _sbSender.SendMessageAsync(new ServiceBusMessage(JsonSerializer.Serialize(fetchJob)));
                }

                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                    ex, actions, message, _serviceBusClient, Shared.Constants.ServiceBusConfig.ActivitiesFetchJobs, _logger, cancellationToken);
                return;
            }
        }
    }
}
