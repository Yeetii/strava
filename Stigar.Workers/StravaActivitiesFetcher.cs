using Stigar.Core.Jobs;
using Hangfire;
using Microsoft.Extensions.Logging;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;
using Stigar.Core.Services.StravaClient;

namespace Stigar.Workers
{
public class StravaActivitiesFetcher(ILogger<StravaActivitiesFetcher> _logger, ActivitiesApi _activitiesApi, IActivityRepository _activityRepository, IUserRepository _userRepository, StravaTokenService _stravaTokenService, IBackgroundJobClient jobs, UserSyncStatusService _userSyncStatusService) : IActivitiesFetchJob
{
        public async Task ExecuteAsync(ActivitiesFetchJob fetchJob, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(fetchJob.UserId))
            {
                _logger.LogError("Invalid activities fetch payload: missing userId.");
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
                    "Failed while resolving Strava access token for user {UserId}. Page={Page}, Before={Before}, After={After}",
                    fetchJob.UserId,
                    fetchJob.Page,
                    fetchJob.Before,
                    fetchJob.After);
                throw;
            }

            var page = fetchJob.Page ?? 1;

            IEnumerable<Stigar.Core.Services.StravaClient.Model.SummaryActivity>? activites;
            bool hasMorePages;
            try
            {
                (activites, hasMorePages) = await _activitiesApi.GetActivitiesByAthlete(accessToken, page, fetchJob.Before, fetchJob.After);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed while fetching Strava activities for user {UserId}. Page={Page}, Before={Before}, After={After}",
                    fetchJob.UserId,
                    page,
                    fetchJob.Before,
                    fetchJob.After);
                throw;
            }

            if (activites is null)
            {
                _logger.LogInformation("Strava athlete {UserId} not found (404); stopping.", fetchJob.UserId);
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
                jobs.EnqueueActivitiesFetch(fetchJob);
            }
        }
    }
}
