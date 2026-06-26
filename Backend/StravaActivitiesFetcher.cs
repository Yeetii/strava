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
        private const int MaxAccessTokenRequestAttempts = 3;
        private readonly ServiceBusClient _serviceBusClient = serviceBusClient;
        readonly HttpClient _apiClient = httpClientFactory.CreateClient("backendApiClient");
        readonly ServiceBusSender _activitiesFetchSender = serviceBusClient.CreateSender(Shared.Constants.ServiceBusConfig.ActivitiesFetchJobs);

        [Function(nameof(StravaActivitiesFetcher))]
        public async Task Run(
            [ServiceBusTrigger(Shared.Constants.ServiceBusConfig.ActivitiesFetchJobs, Connection = "ServiceBusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
            ServiceBusMessageActions actions,
            CancellationToken cancellationToken)
        {
            try
            {
                if (!TryParseFetchJob(message, out var fetchJob, out var parseError))
                {
                    var bodyPreview = CreateBodyPreview(message.Body.ToString());
                    _logger.LogError(
                        "Invalid activities fetch payload. MessageId: {MessageId}, ContentType: {ContentType}, ParseError: {ParseError}, BodyPreview: {BodyPreview}",
                        message.MessageId,
                        message.ContentType,
                        parseError,
                        bodyPreview);

                    if (ServiceBusRescheduler.HasRealLockToken(message))
                    {
                        await actions.DeadLetterMessageAsync(
                            message,
                            deadLetterReason: "InvalidActivitiesFetchPayload",
                            deadLetterErrorDescription: parseError,
                            cancellationToken: cancellationToken);
                    }

                    return;
                }

                string accessToken;
                try
                {
                    accessToken = await GetAccessTokenWithRetryAsync(fetchJob.UserId, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed while fetching access token for user {UserId}. MessageId={MessageId}, Page={Page}, Before={Before}, After={After}",
                        fetchJob.UserId,
                        message.MessageId,
                        fetchJob.Page,
                        fetchJob.Before,
                        fetchJob.After);
                    throw;
                }

                var page = fetchJob.Page ?? 1;

                IEnumerable<Shared.Services.StravaClient.Model.SummaryActivity>? activites;
                bool hasMorePages;
                try
                {
                    (activites, hasMorePages) = await _activitiesApi.GetActivitiesByAthlete(accessToken, page, fetchJob.Before, fetchJob.After);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed while fetching Strava activities for user {UserId}. MessageId={MessageId}, Page={Page}, Before={Before}, After={After}",
                        fetchJob.UserId,
                        message.MessageId,
                        page,
                        fetchJob.Before,
                        fetchJob.After);
                    throw;
                }

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

        private static bool TryParseFetchJob(ServiceBusReceivedMessage message, out ActivitiesFetchJob fetchJob, out string error)
        {
            fetchJob = null!;
            error = string.Empty;

            var bodyText = message.Body.ToString();
            if (string.IsNullOrWhiteSpace(bodyText))
            {
                error = "Message body is empty.";
                return false;
            }

            try
            {
                using var jsonDocument = JsonDocument.Parse(bodyText);
                if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
                {
                    error = $"Expected a JSON object with 'userId', but got {jsonDocument.RootElement.ValueKind}.";
                    return false;
                }

                if (jsonDocument.RootElement.TryGetProperty("input", out _))
                {
                    error = "Received admin invoke envelope ('input'). Expected raw ActivitiesFetchJob JSON in the message body: {\"userId\":\"...\"}.";
                    return false;
                }

                var parsed = JsonSerializer.Deserialize<ActivitiesFetchJob>(bodyText, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
                if (parsed is null)
                {
                    error = "Message body could not be deserialized into ActivitiesFetchJob.";
                    return false;
                }

                if (string.IsNullOrWhiteSpace(parsed.UserId))
                {
                    error = "Missing required field: userId.";
                    return false;
                }

                fetchJob = parsed;
                return true;
            }
            catch (JsonException ex)
            {
                error = $"Invalid JSON for ActivitiesFetchJob. {ex.Message}";
                return false;
            }
        }

        private static string CreateBodyPreview(string body)
        {
            const int maxLength = 300;
            if (string.IsNullOrEmpty(body))
                return "<empty>";

            return body.Length <= maxLength ? body : body[..maxLength] + "...";
        }

        private async Task<string> GetAccessTokenWithRetryAsync(string userId, CancellationToken cancellationToken)
        {
            Exception? lastTransientException = null;

            for (var attempt = 1; attempt <= MaxAccessTokenRequestAttempts; attempt++)
            {
                try
                {
                    using var response = await _apiClient.GetAsync($"{userId}/accessToken", cancellationToken);
                    var responseBody = await response.Content.ReadAsStringAsync(cancellationToken);

                    if (!response.IsSuccessStatusCode)
                    {
                        throw new InvalidOperationException(
                            $"Failed to get access token for user {userId}: {(int)response.StatusCode} {response.ReasonPhrase}. Response body: {responseBody}");
                    }

                    return responseBody;
                }
                catch (Exception ex) when (attempt < MaxAccessTokenRequestAttempts && IsTransientConnectionError(ex))
                {
                    lastTransientException = ex;
                    await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt), cancellationToken);
                }
            }

            throw lastTransientException ?? new InvalidOperationException($"Unreachable retry state for access token request for user {userId}.");
        }

        private static bool IsTransientConnectionError(Exception ex)
        {
            if (ex is HttpRequestException httpRequestException
                && (HasTransientConnectionCause(httpRequestException) || IsTransientHttpRequestMessage(httpRequestException)))
                return true;
            if (ex is IOException or System.Net.Sockets.SocketException or System.Security.Authentication.AuthenticationException)
                return true;

            return false;
        }

        private static bool HasTransientConnectionCause(Exception ex)
        {
            for (Exception? current = ex; current is not null; current = current.InnerException)
            {
                if (current is IOException or System.Net.Sockets.SocketException or System.Security.Authentication.AuthenticationException)
                    return true;
            }

            return false;
        }

        private static bool IsTransientHttpRequestMessage(HttpRequestException ex)
        {
            return ex.Message.Contains("Error while copying content to a stream", StringComparison.Ordinal)
                || ex.Message.Contains("The response ended prematurely", StringComparison.Ordinal)
                || ex.Message.Contains("The SSL connection could not be established", StringComparison.Ordinal);
        }
    }
}
