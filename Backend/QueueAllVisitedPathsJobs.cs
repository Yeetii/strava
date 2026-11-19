using Hangfire;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Admin;

namespace Backend
{
    public class QueueAllVisitedPathsJobs(IBackendAdminQueries queries, IActivityRepository activities, IBackgroundJobClient jobs)
    {
        [Function(nameof(QueueAllVisitedPathsJobs))]
        public async Task Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            await QueueActivityCollectionJobs.QueueActivitiesExcludingTypesAsync(
                queries,
                activities,
                jobs,
                QueueConfig.CalculateVisitedPathsJobs,
                ActivityTypeFilters.ExcludedFromPaths,
                ActivitySyncStage.VisitedPaths,
                GetUserIdFilter(req.Url));
        }

        internal static string? GetUserIdFilter(Uri requestUrl)
        {
            var queryString = System.Web.HttpUtility.ParseQueryString(requestUrl.Query);
            var userId = queryString["userId"];
            return string.IsNullOrWhiteSpace(userId) ? null : userId;
        }
    }
}
