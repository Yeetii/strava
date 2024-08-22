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
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();
        var activities = JsonSerializer.Deserialize<List<SummaryActivity>>(body);

        bool hasMorePages = activities?.Count == ActivitesPerPage;

        return (activities, hasMorePages);
    }

    public async Task<DetailedActivity?> GetActivity(string token, string activityId){
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
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();

        var activity = JsonSerializer.Deserialize<DetailedActivity>(body);

        return activity;
    }
}