using Shared.Models;
using Shared.Services.StravaClient;

namespace Backend.Tests;

public class StravaActivityFetchSchedulingTests
{
    [Fact]
    public void ResolveRetryScheduleUtc_UsesTomorrowsFinalHour_ForHistoricalDailyLimit()
    {
        var nowUtc = new DateTimeOffset(2026, 5, 13, 12, 0, 0, TimeSpan.Zero);
        var activity = CreateActivity(nowUtc.UtcDateTime.AddMonths(-7));
        var exception = CreateRateLimitException(retryAtUtc: new DateTimeOffset(2026, 5, 14, 0, 0, 0, TimeSpan.Zero), isDailyLimitExceeded: true);

        var scheduleUtc = StravaActivityFetchScheduling.ResolveRetryScheduleUtc(exception, activity, "12345", nowUtc);

        Assert.Equal(new DateTimeOffset(2026, 5, 14, 23, 0, 0, TimeSpan.Zero).Date, scheduleUtc.Date);
        Assert.Equal(23, scheduleUtc.Hour);
        Assert.InRange(scheduleUtc.Minute * 60 + scheduleUtc.Second, 0, 3599);
    }

    [Fact]
    public void ResolveRetryScheduleUtc_KeepsStravaRetry_ForRecentActivity()
    {
        var retryAtUtc = new DateTimeOffset(2026, 5, 14, 0, 0, 0, TimeSpan.Zero);
        var nowUtc = new DateTimeOffset(2026, 5, 13, 12, 0, 0, TimeSpan.Zero);
        var activity = CreateActivity(nowUtc.UtcDateTime.AddMonths(-1));
        var exception = CreateRateLimitException(retryAtUtc, isDailyLimitExceeded: true);

        var scheduleUtc = StravaActivityFetchScheduling.ResolveRetryScheduleUtc(exception, activity, "12345", nowUtc);

        Assert.Equal(retryAtUtc, scheduleUtc);
    }

    private static Activity CreateActivity(DateTime startDateUtc) => new()
    {
        Id = "activity-1",
        UserId = "user-1",
        Name = "Test Activity",
        SportType = SportTypes.RUN,
        StartDate = startDateUtc,
        StartDateLocal = startDateUtc,
    };

    private static StravaRateLimitExceededException CreateRateLimitException(DateTimeOffset retryAtUtc, bool isDailyLimitExceeded)
        => new(
            retryAtUtc,
            isDailyLimitExceeded,
            isFifteenMinuteLimitExceeded: !isDailyLimitExceeded,
            limit: (100, 3000),
            usage: isDailyLimitExceeded ? (10, 3000) : (100, 10),
            responseBody: null);
}