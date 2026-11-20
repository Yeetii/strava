using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;

namespace Stigar.Api
{
    public class GetSkiDays(ILoggerFactory loggerFactory, IActivityRepository activitiesRepository)
    {
        private readonly ILogger _logger = loggerFactory.CreateLogger<GetSkiDays>();

        public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
            app.MapGet("/api/{userId}/skiDays",
                (HttpContext context, [FromServices] GetSkiDays endpoint, string userId) => endpoint.Handle(context, userId, context.RequestAborted));

        public async Task<IResult> Handle(HttpContext context, string userId, CancellationToken cancellationToken)
        {
            IEnumerable<Activity> activities = await activitiesRepository.GetAllByUser(userId);

            string? before = context.Request.Query["before"].Count > 0 ? context.Request.Query["before"].ToString() : null;
            string? after = context.Request.Query["after"].Count > 0 ? context.Request.Query["after"].ToString() : null;

            if (before != null)
                activities = activities.Where(a => a.StartDate <= DateTime.Parse(before));

            if (after != null)
                activities = activities.Where(b => b.StartDate > DateTime.Parse(after));

            var skiDays = CalculateSkiDays(activities);
            return TypedResults.Ok(skiDays);
        }

        public static SkiDays CalculateSkiDays(IEnumerable<Activity> activities)
        {
            var alpineSkiDays = activities.Where(activity => activity.SportType == SportTypes.ALPINE_SKIING)
                .Select(activity => activity.StartDate.Date).Distinct();
            var backcountrySkiDays = activities.Where(activity => activity.SportType == SportTypes.BACKCOUNTRY_SKIING)
                .Select(activity => activity.StartDate.Date).Distinct();
            var nordicSkiDays = activities.Where(activity => activity.SportType == SportTypes.NORDIC_SKIING)
                .Select(activity => activity.StartDate.Date).Distinct();
            var snowboardingDays = activities.Where(activity => activity.SportType == SportTypes.SNOWBOARDING)
                .Select(activity => activity.StartDate.Date).Distinct();

            var skiSports = new List<string>{SportTypes.ALPINE_SKIING, SportTypes.BACKCOUNTRY_SKIING, SportTypes.NORDIC_SKIING, SportTypes.SNOWBOARDING};

            var totalSkiDays = activities.Where(activity => skiSports.Contains(activity.SportType))
                .Select(activity => activity.StartDate.Date).Distinct();

            var backcountrySkiElevationGain = activities.Where(activity => activity.SportType == SportTypes.BACKCOUNTRY_SKIING)
                .Sum(activity => (double)(activity.TotalElevationGain ?? 0));

            return new SkiDays
            {
                AlpineSkiDays = alpineSkiDays.Count(),
                BackcountrySkiDays = backcountrySkiDays.Count(),
                NordicSkiDays = nordicSkiDays.Count(),
                SnowboardDays = snowboardingDays.Count(),
                TotalSkiDays = totalSkiDays.Count(),
                BackcountrySkiElevationGain = backcountrySkiElevationGain
            };
        }
    }
}
