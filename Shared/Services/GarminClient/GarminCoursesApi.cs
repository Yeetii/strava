using System.Net.Http.Headers;

namespace Shared.Services.GarminClient;

public class GarminCoursesApi(HttpClient garminProxyClient)
{

    public Task<HttpResponseMessage> GetCourses(string? activityType, int start, int limit, CancellationToken cancellationToken)
    {
        var queryParts = new List<string>
        {
            $"start={Uri.EscapeDataString(start.ToString())}",
            $"limit={Uri.EscapeDataString(limit.ToString())}"
        };

        if (!string.IsNullOrWhiteSpace(activityType))
            queryParts.Add($"type={Uri.EscapeDataString(activityType)}");

        return SendAsync(HttpMethod.Get, $"garmin/courses?{string.Join("&", queryParts)}", cancellationToken: cancellationToken);
    }

    public Task<HttpResponseMessage> GetCourse(string courseId, CancellationToken cancellationToken)
        => SendAsync(HttpMethod.Get, $"garmin/courses/{Uri.EscapeDataString(courseId)}", cancellationToken: cancellationToken);

    public Task<HttpResponseMessage> DownloadCourseGpx(string courseId, CancellationToken cancellationToken)
        => SendAsync(HttpMethod.Get, $"garmin/courses/{Uri.EscapeDataString(courseId)}/gpx", cancellationToken: cancellationToken);

    public Task<HttpResponseMessage> DeleteCourse(string courseId, CancellationToken cancellationToken)
        => SendAsync(HttpMethod.Delete, $"garmin/courses/{Uri.EscapeDataString(courseId)}", cancellationToken: cancellationToken);

    public Task<HttpResponseMessage> UpdateCourse(string courseId, byte[] gpxBytes, string fileName, CancellationToken cancellationToken)
    {
        var content = new ByteArrayContent(gpxBytes);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/gpx+xml");

        return SendAsync(
            HttpMethod.Put,
            $"garmin/courses/{Uri.EscapeDataString(courseId)}?filename={Uri.EscapeDataString(fileName)}",
            content,
            cancellationToken);
    }

    public Task<HttpResponseMessage> UploadCourse(byte[] gpxBytes, string fileName, CancellationToken cancellationToken)
    {
        var content = new ByteArrayContent(gpxBytes);
        content.Headers.ContentType = MediaTypeHeaderValue.Parse("application/gpx+xml");

        return SendAsync(
            HttpMethod.Post,
            $"garmin/courses?filename={Uri.EscapeDataString(fileName)}",
            content,
            cancellationToken);
    }

    private Task<HttpResponseMessage> SendAsync(
        HttpMethod method,
        string relativePath,
        HttpContent? content = null,
        CancellationToken cancellationToken = default)
    {
        var request = new HttpRequestMessage(method, relativePath)
        {
            Content = content
        };

        return garminProxyClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
    }
}