using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Logging;
using Stigar.Core.Models;
using Stigar.Core.Services.Postgres;

namespace Stigar.Api;

public class Streaks
{
    public int CurrentActivityStreak { get; set; }
    public int LongestActivityStreak { get; set; }
    public DateTime CurrentActivityStreakStartDate { get; set; }
    public DateTime LongestActivityStreakStartDate { get; set; }
    public DateTime LongestActivityStreakEndDate { get; set; }
    public int CurrentRunningStreak { get; set; }
    public int LongestRunningStreak { get; set; }
    public DateTime CurrentRunningStreakStartDate { get; set; }
    public DateTime LongestRunningStreakStartDate { get; set; }
    public DateTime LongestRunningStreakEndDate { get; set; }
}


public class GetStreaks(ILoggerFactory loggerFactory, IActivityRepository activitiesRepository)
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<GetStreaks>();

    public static RouteHandlerBuilder Map(IEndpointRouteBuilder app) =>
        app.MapGet("/api/{userId}/streaks",
            (HttpContext context, [FromServices] GetStreaks endpoint, string userId) => endpoint.Handle(context, userId, context.RequestAborted));

    public async Task<IResult> Handle(HttpContext context, string userId, CancellationToken cancellationToken)
    {
        IEnumerable<Activity> activities = await activitiesRepository.GetAllByUser(userId);

        string? before = context.Request.Query["before"].Count > 0 ? context.Request.Query["before"].ToString() : null;
        string? after = context.Request.Query["after"].Count > 0 ? context.Request.Query["after"].ToString() : null;

        if (before != null)
            activities = activities.Where(a => a.StartDate <= DateTime.Parse(before));

        if (after != null)
            activities = activities.Where(b => b.StartDate > DateTime.Parse(after));

        var streaks = StreakCalculator.CalculateStreaks(activities);
        return TypedResults.Ok(streaks);
        
    }
}

public class StreakCalculator
{
    public static Streaks CalculateStreaks(IEnumerable<Activity> activities)
    {

        var streaks = new Streaks
        {
            CurrentActivityStreak = CalculateStreak(activities),
            LongestActivityStreak = 0,
            CurrentActivityStreakStartDate = new DateTime(),
            LongestActivityStreakStartDate = new DateTime(),
            LongestActivityStreakEndDate = new DateTime(),
            CurrentRunningStreak = CalculateStreak(activities, [SportTypes.RUN, SportTypes.TRAIL_RUN, SportTypes.VIRTUAL_RUN]),
            LongestRunningStreak = 0,
            CurrentRunningStreakStartDate = new DateTime(),
            LongestRunningStreakEndDate = new DateTime(),
            LongestRunningStreakStartDate = new DateTime(),
        };
        return streaks;
    }

    private static bool DatesAreOnSameDay(DateTime first, DateTime second)
    {
        return first.Year == second.Year && first.Month == second.Month && first.Day == second.Day;
    }

    public static int CalculateStreak(IEnumerable<Activity> allActivities, IEnumerable<string>? activityTypeFilter = default)
    {
        List<Activity> activities;
        if (activityTypeFilter != null)
        {
            activities = allActivities.Where(activity => activityTypeFilter.Contains(activity.SportType)).ToList();
        }
        else
        {
            activities = allActivities.ToList();
        }

        if (activities.Count < 1)
            return 0;

        var activityDates = activities.Select(x => x.StartDate).ToList();
        var activityToday = DatesAreOnSameDay(DateTime.Now, activityDates[^1]) ? 1 : 0;

        var yesterday = DateTime.Now.AddDays(-1);
        return activityToday + CalculateStreakRecursive(activityDates, yesterday);
    }

    private static int CalculateStreakRecursive(List<DateTime> runDates, DateTime currentDay)
    {
        if (runDates.Count == 0)
        {
            return 0;
        }

        var lastRun = runDates.Last();
        runDates.RemoveAt(runDates.Count - 1);

        if (DatesAreOnSameDay(lastRun, currentDay))
        {
            var prevDay = currentDay.AddDays(-1);
            return 1 + CalculateStreakRecursive(runDates, prevDay);
        }
        else if (currentDay < lastRun)
        {
            return CalculateStreakRecursive(runDates, currentDay);
        }

        return 0;
    }
}