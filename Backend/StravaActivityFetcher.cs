using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;

namespace Backend
{
    public class ActivityFetchJob
    {
        public required string UserId { get; set; }
        public required string ActivityId { get; set; }
    }

    public class StravaActivityFetcher(ILogger<StravaActivityFetcher> _logger, IHttpClientFactory httpClientFactory, ActivitiesApi _activitiesApi, CollectionClient<Activity> _activitiesCollection, ServiceBusClient serviceBusClient)
    {
        private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
        readonly HttpClient _backendApiClient = httpClientFactory.CreateClient("backendApiClient");

        [Function(nameof(StravaActivityFetcher))]
        public async Task Run(
            [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.ActivityFetchJobs, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
            ServiceBusMessageActions actions,
            CancellationToken cancellationToken)
        {
            var fetchJob = message.Body.ToObjectFromJson<ActivityFetchJob>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            try
            {
                var accessTokenResponse = await _backendApiClient.GetAsync($"{fetchJob.UserId}/accessToken");

                if (!accessTokenResponse.IsSuccessStatusCode)
                {
                    var responseBody = await accessTokenResponse.Content.ReadAsStringAsync();
                    throw new InvalidOperationException(
                        $"Failed to get access token for user {fetchJob.UserId}, activity {fetchJob.ActivityId}: {(int)accessTokenResponse.StatusCode} {accessTokenResponse.ReasonPhrase}. Response body: {responseBody}");
                }

                var accessToken = await accessTokenResponse.Content.ReadAsStringAsync();
                var activity = await _activitiesApi.GetActivity(accessToken, fetchJob.ActivityId);
                if (activity is null)
                {
                    _logger.LogInformation("Strava activity {ActivityId} not found (404); completing message.", fetchJob.ActivityId);
                    await actions.CompleteMessageAsync(message, cancellationToken);
                    return;
                }

                await _activitiesCollection.UpsertDocument(ActivityMapper.MapDetailedActivity(activity));
                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                await ServiceBusCosmosRetryHelper.HandleRetryAsync(
                    ex, actions, message, _serviceBusClient, Shared.Constants.ServiceBusConfig.ActivityFetchJobs, _logger, cancellationToken);
            }
        }
    }
}
