using Shared.Jobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.StravaClient;
using System.Text.Json;

namespace Backend
{
public class StravaActivitiesFetcher(ILogger<StravaActivitiesFetcher> _logger, ActivitiesApi _activitiesApi, IActivityRepository _activityRepository, IUserRepository _userRepository, StravaTokenService _stravaTokenService, QueueClient queueClient, UserSyncStatusService _userSyncStatusService)
{
        private readonly QueueClient _queueClient = queueClient;
        readonly QueueSender _activitiesFetchSender = queueClient.CreateSender(Shared.Constants.QueueConfig.ActivitiesFetchJobs);

        [Function(nameof(StravaActivitiesFetcher))]
        public async Task Run(
            [QueueTrigger(Shared.Constants.QueueConfig.ActivitiesFetchJobs, AutoCompleteMessages = false)] QueueMessage message,
            QueueMessageActions actions,
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

                    if (QueueMessageGuard.HasLockToken(message))
                    {
                        await actions.DeadLetterMessageAsync(
                            message,
                            deadLetterReason: "InvalidActivitiesFetchPayload",
                            deadLetterErrorDescription: parseError,
                            cancellationToken: cancellationToken);
                    }

                    return;
                }

                string? accessToken;
                try
                {
                    var user = await _userRepository.GetById(fetchJob.UserId, cancellationToken);
                    accessToken = user is null
                        ? null
                        : await _stravaTokenService.GetValidAccessToken(user);
                    if (string.IsNullOrWhiteSpace(accessToken))
                    {
                        throw new InvalidOperationException(
                            $"No valid Strava access token available for user {fetchJob.UserId}.");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed while resolving Strava access token for user {UserId}. MessageId={MessageId}, Page={Page}, Before={Before}, After={After}",
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
                var existingActivities = (await _activityRepository.GetByIds(activityIds, cancellationToken))
                    .ToDictionary(activity => activity.Id, StringComparer.Ordinal);

                var newActivityCount = activityIds.Count(id => !existingActivities.ContainsKey(id));
                var mappedActivities = activitiesList.Select(activity =>
                {
                    existingActivities.TryGetValue(activity.Id.ToString(), out var existingActivity);
                    return ActivityMapper.MapSummaryActivity(activity, existingActivity);
                });

                await _activityRepository.BulkUpsert(
                    mappedActivities,
                    cancellationToken: cancellationToken);
                await _userSyncStatusService.IncrementSyncedActivities(fetchJob.UserId, newActivityCount, cancellationToken);

                var discoveredMinimumTotal = ((page - 1) * 200) + activitiesList.Count;
                await _userSyncStatusService.SetTotalActivitiesOnStravaAtLeast(fetchJob.UserId, discoveredMinimumTotal, cancellationToken);

                if (hasMorePages)
                {
                    fetchJob.Page = ++page;
                    await _activitiesFetchSender.SendMessageAsync(new QueueMessage(JsonSerializer.Serialize(fetchJob)), cancellationToken);
                }

                await actions.CompleteMessageAsync(message, cancellationToken);
            }
            catch (Exception ex)
            {
                throw;

            }
        }

        private static bool TryParseFetchJob(QueueMessage message, out ActivitiesFetchJob fetchJob, out string error)
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

    }
}
