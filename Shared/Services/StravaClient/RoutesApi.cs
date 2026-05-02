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

    public async Task<Stream?> GetRouteGpx(string token, long routeId)
    {
        var requestUri = $"routes/{routeId}/export_gpx";
        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Get,
            RequestUri = new Uri(_stravaClient.BaseAddress + requestUri),
            Headers = { { "Authorization", $"Bearer {token}" } }
        };

        var response = await _stravaClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            response.Dispose();
            return null;
        }

        response.EnsureSuccessStatusCode();
        return await response.Content.ReadAsStreamAsync();
    }

    public async Task<UploadStatus> UploadActivity(string token, Stream fileContent, string filename, string name, string dataType, string? description = null)
    {
        using var content = new MultipartFormDataContent();
        var streamContent = new StreamContent(fileContent);
        streamContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        content.Add(streamContent, "file", filename);
        content.Add(new StringContent(name), "name");
        content.Add(new StringContent(dataType), "data_type");
        if (!string.IsNullOrEmpty(description))
            content.Add(new StringContent(description), "description");

        var request = new HttpRequestMessage
        {
            Method = HttpMethod.Post,
            RequestUri = new Uri(_stravaClient.BaseAddress + "uploads"),
            Headers = { { "Authorization", $"Bearer {token}" } },
            Content = content
        };

        using var response = await _stravaClient.SendAsync(request);
        response.EnsureSuccessStatusCode();
        var body = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<UploadStatus>(body)
            ?? throw new JsonException($"Could not parse upload response. Body: {body}");
    }
}
