using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Microsoft.AspNetCore.Mvc;
using Shared.Models;

namespace API;

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


public class GetStreaks(ILoggerFactory loggerFactory)
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<GetStreaks>();

    [OpenApiOperation(tags: ["Aggregates"])]
    [OpenApiParameter(name: "userId", In = ParameterLocation.Path)]
    [OpenApiParameter(name: "before", In = ParameterLocation.Query)]
    [OpenApiParameter(name: "after", In = ParameterLocation.Query)]
    [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
    [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "application/json", bodyType: typeof(Streaks), Description = "The OK response")]
    [Function("GetStreaks")]
    public IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "{userId}/streaks")] HttpRequestData req, string userId,
    [CosmosDBInput(
        databaseName: "%CosmosDb%",
        containerName: "%ActivitiesContainer%",
        Connection  = "CosmosDBConnection",
        SqlQuery = "SELECT * FROM c where c.userId = {userId}"
        )] IEnumerable<Activity> activities)
    {
        var before = req.Query["before"];
        var after = req.Query["after"];

        if (before != null)
            activities = activities.Where(a => a.StartDate <= DateTime.Parse(before));

        if (after != null)
            activities = activities.Where(b => b.StartDate > DateTime.Parse(after));

        var streaks = StreakCalculator.CalculateStreaks(activities);
        return new JsonResult(streaks);
        
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