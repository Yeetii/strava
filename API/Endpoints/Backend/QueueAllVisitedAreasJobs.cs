using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Builder;
using Hangfire;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Admin;

namespace API.Endpoints.Backend
{
    public class QueueAllVisitedAreasJobs(IBackendAdminQueries queries, IActivityRepository activities, IBackgroundJobClient jobs)
    {
        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapMethods("/api/QueueAllVisitedAreasJobs", ["GET", "POST"], (HttpContext context, [FromServices] QueueAllVisitedAreasJobs endpoint) => endpoint.Handle(context));

        public async Task<IResult> Handle(HttpContext context)
        {
            await QueueActivityCollectionJobs.QueueActivitiesExcludingTypesAsync(
                queries,
                activities,
                jobs,
                QueueConfig.CalculateVisitedAreasJobs,
                ActivityTypeFilters.ExcludedFromAreas,
                ActivitySyncStage.VisitedAreas,
                GetUserIdFilter(context));
            return TypedResults.NoContent();
        }

        internal static string? GetUserIdFilter(HttpContext context)
        {
            var queryString = context.Request.Query;
            var userId = queryString["userId"].ToString();
            return string.IsNullOrWhiteSpace(userId) ? null : userId;
        }
    }
}
