using System.Text.Json;
using Shared.Services.StravaClient.Model;

namespace Shared.Services.StravaClient;

public static class ActivitiesAPI
{
    private static readonly int ActivitesPerPage = 200;
    public async static Task<(IEnumerable<SummaryActivity>? activites, bool hasMorePages)> GetStravaModel(string token, int page = 1, DateTime? before = null, DateTime? after = null)
    {
        var client = new HttpClient();

        var requestUri = $"https://www.strava.com/api/v3/athlete/activities?per_page={ActivitesPerPage}&page={page}";

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
            RequestUri = new Uri(requestUri),
            Headers =
                {
                    { "Authorization", $"Bearer {token}" },
                },
        };

        using var response = await client.SendAsync(request);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();
        var activities = JsonSerializer.Deserialize<List<SummaryActivity>>(body);

        bool hasMorePages = activities?.Count == ActivitesPerPage;

        return (activities, hasMorePages);
    }
}