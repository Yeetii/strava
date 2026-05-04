using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Shared.Services.StravaClient.Model;

namespace Shared.Services.StravaClient;

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

    public async Task<StravaRoute> CreateRoute(string token, Stream fileContent, string filename, string name, int type, int subType, string? description = null)
    {
        using var content = new MultipartFormDataContent();
        var streamContent = new StreamContent(fileContent);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        content.Add(streamContent, "file", filename);
        content.Add(new StringContent(name), "name");
        content.Add(new StringContent(type.ToString()), "type");
        content.Add(new StringContent(subType.ToString()), "sub_type");
        if (!string.IsNullOrEmpty(description))
            content.Add(new StringContent(description), "description");

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            RequestUri = new Uri(_stravaClient.BaseAddress + "routes"),
            Headers = { { "Authorization", $"Bearer {token}" } },
            Content = content
        };

        using var response = await _stravaClient.SendAsync(request);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<StravaRoute>(body)
            ?? throw new JsonException($"Could not parse create route response. Body: {body}");
    }
}
