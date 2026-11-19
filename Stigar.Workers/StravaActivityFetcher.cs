using Microsoft.Extensions.Logging;
using Stigar.Core.Logging;
using Stigar.Core.Jobs;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.StravaClient;

namespace Stigar.Workers
{
    public class StravaActivityFetcher(ILogger<StravaActivityFetcher> _logger, ActivitiesApi _activitiesApi, IActivityRepository _activityRepository, IUserRepository _userRepository, StravaTokenService _stravaTokenService, UserSyncStatusService _userSyncStatusService) : IActivityFetchJob
    {
        public async Task ExecuteAsync(ActivityFetchJob fetchJob, CancellationToken cancellationToken)
        {
            // IDs are used verbatim for lookups but sanitized for logs so an
            // attacker-controlled value cannot forge extra log lines.
            var safeUserId = LogInput.Sanitize(fetchJob.UserId);
            var safeActivityId = LogInput.Sanitize(fetchJob.ActivityId);
            try
            {
                if (string.IsNullOrWhiteSpace(fetchJob.UserId) || string.IsNullOrWhiteSpace(fetchJob.ActivityId))
                {
                    _logger.LogError(
                        "Invalid activity fetch payload. UserId: {UserId}, ActivityId: {ActivityId}",
                        safeUserId,
                        safeActivityId);
                    return;
                }

                var user = await _userRepository.GetById(fetchJob.UserId, cancellationToken);
                var accessToken = user is null
                    ? null
                    : await _stravaTokenService.GetValidAccessToken(user);
                if (string.IsNullOrWhiteSpace(accessToken))
                {
                    throw new InvalidOperationException(
                        $"Failed to get a valid Strava access token for user {safeUserId}, activity {safeActivityId}.");
                }

                var existingActivity = await _activityRepository.GetById(fetchJob.ActivityId, cancellationToken);
                var activity = await _activitiesApi.GetActivity(accessToken, fetchJob.ActivityId);
                if (activity is null)
                {
                    _logger.LogInformation("Strava activity {ActivityId} not found (404); stopping.", safeActivityId);
                    return;
                }

                await _activityRepository.Upsert(
                    ActivityMapper.MapDetailedActivity(activity, existingActivity),
                    cancellationToken: cancellationToken);
                if (existingActivity == null)
                {
                    await _userSyncStatusService.IncrementSyncedActivities(fetchJob.UserId, 1, cancellationToken);
                    await _userSyncStatusService.IncrementKnownTotalActivitiesOnStrava(fetchJob.UserId, 1, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fetch Strava activity {ActivityId} for user {UserId}",
                    safeActivityId, safeUserId);
                throw;
            }
        }
    }
}
