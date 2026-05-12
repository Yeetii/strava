using System.Net;
using System.Text.Json;
using Shared.Services.StravaClient.Model;

namespace Shared.Services.StravaClient;

public class ActivitiesApi(HttpClient _stravaClient)
{
    private static readonly int ActivitesPerPage = 200;

    public async Task<(IEnumerable<SummaryActivity>? activites, bool hasMorePages)> GetActivitiesByAthlete(string token, int page = 1, DateTime? before = null, DateTime? after = null)
    {

        var requestUri = $"athlete/activities?per_page={ActivitesPerPage}&page={page}";

        if (after.HasValue)
        {
            var epoch = new DateTimeOffset(after.Value).ToUnixTimeSeconds();
            requestUri += $"&after={epoch}";
        }

        if (before.HasValue)
        {
            var epoch = new DateTimeOffset(before.Value).ToUnixTimeSeconds();
            requestUri += $"&before={epoch}";
        }

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(_stravaClient.BaseAddress + requestUri),
            Headers =
                {
                    { "Authorization", $"Bearer {token}" },
                },
        };

        using var response = await _stravaClient.SendAsync(request);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return (null, false);

        if (response.StatusCode == HttpStatusCode.TooManyRequests)
        {
            var responseBody = await response.Content.ReadAsStringAsync();
            throw CreateRateLimitExceededException(response, responseBody);
        }

        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();
        var activities = JsonSerializer.Deserialize<List<SummaryActivity>>(body);

        bool hasMorePages = activities?.Count == ActivitesPerPage;

        return (activities, hasMorePages);
    }

    public async Task<DetailedActivity?> GetActivity(string token, string activityId)
    {
        var requestUri = $"activities/{activityId}";
        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(_stravaClient.BaseAddress + requestUri),
            Headers =
                {
                    { "Authorization", $"Bearer {token}" },
                },
        };
        using var response = await _stravaClient.SendAsync(request);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return null;

        if (response.StatusCode == HttpStatusCode.TooManyRequests)
        {
            var responseBody = await response.Content.ReadAsStringAsync();
            throw CreateRateLimitExceededException(response, responseBody);
        }

        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();

        var activity = JsonSerializer.Deserialize<DetailedActivity>(body);

        return activity;
    }

    private static StravaRateLimitExceededException CreateRateLimitExceededException(HttpResponseMessage response, string responseBody)
    {
        var snapshot =
            TryGetRateLimitSnapshot(response, "X-ReadRateLimit-Limit", "X-ReadRateLimit-Usage")
            ?? TryGetRateLimitSnapshot(response, "X-RateLimit-Limit", "X-RateLimit-Usage");

        var now = DateTimeOffset.UtcNow;
        var isDailyLimitExceeded = snapshot is { } value && value.Usage.Daily >= value.Limit.Daily;
        var isFifteenMinuteLimitExceeded = snapshot is { } fifteenMinuteValue
            && fifteenMinuteValue.Usage.FifteenMinute >= fifteenMinuteValue.Limit.FifteenMinute;
        var retryAtUtc = StravaActivityFetchScheduling.GetRateLimitRetryUtc(
            isDailyLimitExceeded,
            isFifteenMinuteLimitExceeded,
            now);

        return new StravaRateLimitExceededException(
            retryAtUtc,
            isDailyLimitExceeded,
            isFifteenMinuteLimitExceeded,
            snapshot?.Limit,
            snapshot?.Usage,
            responseBody);
    }

    private static ((int FifteenMinute, int Daily) Limit, (int FifteenMinute, int Daily) Usage)? TryGetRateLimitSnapshot(
        HttpResponseMessage response,
        string limitHeaderName,
        string usageHeaderName)
    {
        if (!TryParseRateLimitHeader(response, limitHeaderName, out var limit)
            || !TryParseRateLimitHeader(response, usageHeaderName, out var usage))
        {
            return null;
        }

        return (limit, usage);
    }

    private static bool TryParseRateLimitHeader(
        HttpResponseMessage response,
        string headerName,
        out (int FifteenMinute, int Daily) values)
    {
        values = default;
        if (!response.Headers.TryGetValues(headerName, out var headers))
            return false;

        var raw = headers.FirstOrDefault();
        if (string.IsNullOrWhiteSpace(raw))
            return false;

        var parts = raw.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2
            || !int.TryParse(parts[0], out var fifteenMinute)
            || !int.TryParse(parts[1], out var daily))
        {
            return false;
        }

        values = (fifteenMinute, daily);
        return true;
    }
}