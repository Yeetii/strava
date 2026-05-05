using System.Net;
using System.Text.Json;
using Shared.Services.StravaClient.Model;

namespace Shared.Services.StravaClient;

public enum DeleteRouteResult
{
    Deleted,
    NotFound,
    Unauthorized
}

public class RoutesApi(HttpClient _stravaClient)
{
    public async Task<IReadOnlyList<StravaRoute>?> GetAthleteRoutes(string token, string athleteId, int page = 1, int perPage = 200)
    {
        var requestUri = $"athletes/{athleteId}/routes?page={page}&per_page={perPage}";
        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(_stravaClient.BaseAddress + requestUri),
            Headers = { { "Authorization", $"Bearer {token}" } }
        };

        using var response = await _stravaClient.SendAsync(request);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return null;

        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<List<StravaRoute>>(body);
    }

    public async Task<Stream?> GetRouteGpx(string token, string routeId)
    {
        var requestUri = $"routes/{routeId}/export_gpx";
        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(_stravaClient.BaseAddress + requestUri),
            Headers = { { "Authorization", $"Bearer {token}" } }
        };

        using var response = await _stravaClient.SendAsync(request);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return null;

        response.EnsureSuccessStatusCode();
        var memoryStream = new MemoryStream();
        await response.Content.CopyToAsync(memoryStream);
        memoryStream.Position = 0;
        return memoryStream;
    }

    public async Task<DeleteRouteResult> DeleteRoute(string token, string routeId)
    {
        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Delete,
            RequestUri = new Uri(_stravaClient.BaseAddress + $"routes/{routeId}"),
            Headers = { { "Authorization", $"Bearer {token}" } }
        };

        using var response = await _stravaClient.SendAsync(request);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return DeleteRouteResult.NotFound;

        if (response.StatusCode == HttpStatusCode.Unauthorized)
            return DeleteRouteResult.Unauthorized;

        response.EnsureSuccessStatusCode();
        return DeleteRouteResult.Deleted;
    }
}
