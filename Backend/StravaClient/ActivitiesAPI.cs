using System.Text.Json;
using Backend.StravaClient.Model;

namespace Backend.StravaClient;

public static class ActivitiesAPI
{
    public async static Task<IEnumerable<StravaActivity>?> GetStravaModel(string token, int page = 1, DateTime? before = null, DateTime? after = null)
    {
        var client = new HttpClient();
        var activities = new List<StravaActivity>();
        bool hasMorePages = true;

        while (hasMorePages)
        {
            var requestUri = $"https://www.strava.com/api/v3/athlete/activities?per_page=200&page={page}";

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
            var pageActivities = JsonSerializer.Deserialize<List<StravaActivity>>(body);

            if (pageActivities != null)
            {
                activities.AddRange(pageActivities);
                page++;
            }
            if (pageActivities?.Count < 200)
            {
                hasMorePages = false;
            }
        }

        return activities;
    }
}