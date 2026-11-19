using Microsoft.Extensions.Logging;
using Shared.Jobs;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.StravaClient;

namespace Backend
{
    public class StravaActivityFetcher(ILogger<StravaActivityFetcher> _logger, ActivitiesApi _activitiesApi, IActivityRepository _activityRepository, IUserRepository _userRepository, StravaTokenService _stravaTokenService, UserSyncStatusService _userSyncStatusService) : IActivityFetchJob
    {
        public async Task ExecuteAsync(ActivityFetchJob fetchJob, CancellationToken cancellationToken)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(fetchJob.UserId) || string.IsNullOrWhiteSpace(fetchJob.ActivityId))
                {
                    _logger.LogError(
                        "Invalid activity fetch payload. UserId: {UserId}, ActivityId: {ActivityId}",
                        fetchJob.UserId,
                        fetchJob.ActivityId);
                    return;
                }

                var user = await _userRepository.GetById(fetchJob.UserId, cancellationToken);
                var accessToken = user is null
                    ? null
                    : await _stravaTokenService.GetValidAccessToken(user);
                if (string.IsNullOrWhiteSpace(accessToken))
                {
                    throw new InvalidOperationException(
                        $"Failed to get a valid Strava access token for user {fetchJob.UserId}, activity {fetchJob.ActivityId}.");
                }

                var existingActivity = await _activityRepository.GetById(fetchJob.ActivityId, cancellationToken);
                var activity = await _activitiesApi.GetActivity(accessToken, fetchJob.ActivityId);
                if (activity is null)
                {
                    _logger.LogInformation("Strava activity {ActivityId} not found (404); stopping.", fetchJob.ActivityId);
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
                    fetchJob.ActivityId, fetchJob.UserId);
                throw;
            }
        }
    }
}
