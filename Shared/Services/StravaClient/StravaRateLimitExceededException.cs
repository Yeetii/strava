using System.Net;

namespace Shared.Services.StravaClient;

public sealed class StravaRateLimitExceededException : HttpRequestException
{
    public StravaRateLimitExceededException(
        DateTimeOffset retryAtUtc,
        bool isDailyLimitExceeded,
        bool isFifteenMinuteLimitExceeded,
        (int FifteenMinute, int Daily)? limit,
        (int FifteenMinute, int Daily)? usage,
        string? responseBody)
        : base(CreateMessage(retryAtUtc, isDailyLimitExceeded, isFifteenMinuteLimitExceeded, limit, usage, responseBody), null, HttpStatusCode.TooManyRequests)
    {
        RetryAtUtc = retryAtUtc;
        IsDailyLimitExceeded = isDailyLimitExceeded;
        IsFifteenMinuteLimitExceeded = isFifteenMinuteLimitExceeded;
        Limit = limit;
        Usage = usage;
        ResponseBody = responseBody;
    }

    public DateTimeOffset RetryAtUtc { get; }

    public bool IsDailyLimitExceeded { get; }

    public bool IsFifteenMinuteLimitExceeded { get; }

    public (int FifteenMinute, int Daily)? Limit { get; }

    public (int FifteenMinute, int Daily)? Usage { get; }

    public string? ResponseBody { get; }

    private static string CreateMessage(
        DateTimeOffset retryAtUtc,
        bool isDailyLimitExceeded,
        bool isFifteenMinuteLimitExceeded,
        (int FifteenMinute, int Daily)? limit,
        (int FifteenMinute, int Daily)? usage,
        string? responseBody)
    {
        var scope = isDailyLimitExceeded
            ? "daily"
            : isFifteenMinuteLimitExceeded
                ? "15-minute"
                : "unknown";
        var detail = limit.HasValue && usage.HasValue
            ? $" usage={usage.Value.FifteenMinute},{usage.Value.Daily} limit={limit.Value.FifteenMinute},{limit.Value.Daily}"
            : string.Empty;
        var body = string.IsNullOrWhiteSpace(responseBody)
            ? string.Empty
            : $" body={responseBody}";

        return $"Strava rate limit exceeded ({scope}). Retry at {retryAtUtc:O}.{detail}{body}";
    }
}