using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Services.StravaClient;
using System.Text.Json;

namespace Backend
{
    public class StravaActivitiesFetcher(ILogger<StravaActivitiesFetcher> _logger, IHttpClientFactory httpClientFactory, ActivitiesApi _activitiesApi, CollectionClient<Activity> _activitiesCollection, ServiceBusClient serviceBusClient, UserSyncStatusService _userSyncStatusService)
    {
        private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
        readonly HttpClient _apiClient = httpClientFactory.CreateClient("backendApiClient");
        readonly ServiceBusSender _activitiesFetchSender = serviceBusClient.CreateSender(Shared.Constants.ServiceBusConfig.ActivitiesFetchJobs);

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

                var accessToken = await accessTokenResponse.Content.ReadAsStringAsync(cancellationToken);

                var page = fetchJob.Page ?? 1;

                var (activites, hasMorePages) = await _activitiesApi.GetActivitiesByAthlete(accessToken, page, fetchJob.Before, fetchJob.After);
                if (activites is null)
                {
                    _logger.LogInformation("Strava athlete {UserId} not found (404); completing message.", fetchJob.UserId);
                    await actions.CompleteMessageAsync(message, cancellationToken);
                    return;
                }

                var activitiesList = activites.ToList();
                _logger.LogInformation("Fetched {amount} activities", activitiesList.Count);

                var activityIds = activitiesList.Select(activity => activity.Id.ToString()).ToList();
                var existingActivities = (await _activitiesCollection.GetByIdsAsync(activityIds, cancellationToken))
                    .ToDictionary(activity => activity.Id, StringComparer.Ordinal);

                var newActivityCount = activityIds.Count(id => !existingActivities.ContainsKey(id));
                var mappedActivities = activitiesList.Select(activity =>
                {
                    existingActivities.TryGetValue(activity.Id.ToString(), out var existingActivity);
                    return ActivityMapper.MapSummaryActivity(activity, existingActivity);
                });

                await _activitiesCollection.BulkUpsert(
                    mappedActivities,
                    cancellationToken: cancellationToken,
                    priority: CosmosWritePriority.High);
                await _userSyncStatusService.IncrementSyncedActivities(fetchJob.UserId, newActivityCount, cancellationToken);

                var discoveredMinimumTotal = ((page - 1) * 200) + activitiesList.Count;
                await _userSyncStatusService.SetTotalActivitiesOnStravaAtLeast(fetchJob.UserId, discoveredMinimumTotal, cancellationToken);

                if (hasMorePages)
                {
                    fetchJob.Page = ++page;
                    await _activitiesFetchSender.SendMessageAsync(new ServiceBusMessage(JsonSerializer.Serialize(fetchJob)), cancellationToken);
                }

                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                var scheduledEnqueueTimeUtc = (ex as StravaRateLimitExceededException)?.RetryAtUtc;
                await ServiceBusRescheduler.HandleRetryAsync(
                    ex, actions, message, _serviceBusClient, Shared.Constants.ServiceBusConfig.ActivitiesFetchJobs, _logger, cancellationToken, maxRetryCount: 3, scheduledEnqueueTimeUtc: scheduledEnqueueTimeUtc);
                return;
            }
        }
    }
}
