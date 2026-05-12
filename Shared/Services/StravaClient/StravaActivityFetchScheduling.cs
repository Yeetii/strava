using Shared.Models;

namespace Shared.Services.StravaClient;

public static class StravaActivityFetchScheduling
{
    private const int FinalHourUtc = 23;

    public static DateTimeOffset ResolveRetryScheduleUtc(
        StravaRateLimitExceededException exception,
        Activity? existingActivity,
        string activityId,
        DateTimeOffset nowUtc)
    {
        if (exception.IsDailyLimitExceeded && IsHistoricalActivity(existingActivity, nowUtc))
        {
            return GetFinalHourSlotUtc(GetTomorrowFinalHourWindowStartUtc(nowUtc), activityId);
        }

        return exception.RetryAtUtc;
    }

    public static DateTimeOffset GetRateLimitRetryUtc(
        bool isDailyLimitExceeded,
        bool isFifteenMinuteLimitExceeded,
        DateTimeOffset nowUtc)
    {
        if (isDailyLimitExceeded)
            return GetNextMidnightUtc(nowUtc);

        if (isFifteenMinuteLimitExceeded)
            return GetNextQuarterHourUtc(nowUtc);

        return GetNextQuarterHourUtc(nowUtc);
    }

    public static bool IsHistoricalActivity(Activity? activity, DateTimeOffset nowUtc)
    {
        if (activity == null)
            return false;

        return activity.StartDate.ToUniversalTime() <= nowUtc.UtcDateTime.AddMonths(-6);
    }

    private static DateTimeOffset GetTomorrowFinalHourWindowStartUtc(DateTimeOffset nowUtc)
    {
        var tomorrow = nowUtc.UtcDateTime.Date.AddDays(1);
        return new DateTimeOffset(tomorrow.Year, tomorrow.Month, tomorrow.Day, FinalHourUtc, 0, 0, TimeSpan.Zero);
    }

    private static DateTimeOffset GetFinalHourSlotUtc(DateTimeOffset windowStartUtc, string activityId) =>
        windowStartUtc.AddSeconds(GetDeterministicSecondOffset(activityId));

    private static DateTimeOffset GetNextQuarterHourUtc(DateTimeOffset nowUtc)
    {
        var quarterStart = new DateTimeOffset(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, nowUtc.Minute / 15 * 15, 0, TimeSpan.Zero);
        return quarterStart.AddMinutes(15);
    }

    private static DateTimeOffset GetNextMidnightUtc(DateTimeOffset nowUtc)
    {
        var nextDay = nowUtc.UtcDateTime.Date.AddDays(1);
        return new DateTimeOffset(nextDay, TimeSpan.Zero);
    }

    private static int GetDeterministicSecondOffset(string activityId)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var character in activityId)
            {
                hash ^= character;
                hash *= 16777619;
            }

            return (int)(hash % (60 * 60));
        }
    }
}