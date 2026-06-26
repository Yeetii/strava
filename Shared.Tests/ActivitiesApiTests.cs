using System.Net;
using System.Net.Http.Headers;
using System.Security.Authentication;
using System.Text;
using Shared.Services.StravaClient;

namespace Shared.Tests;

public class ActivitiesApiTests
{
    [Fact]
    public async Task GetActivity_RetriesOnceWhenResponseBodyReadFails()
    {
        var successJson = """
            {
              "id": 123,
              "athlete": { "id": 42 },
              "name": "Morning Run",
              "type": "Run",
              "sport_type": "Run",
              "start_date": "2024-01-01T00:00:00Z",
              "start_date_local": "2024-01-01T01:00:00Z",
              "timezone": "(GMT+01:00) Europe/Stockholm",
              "map": { "id": "m1", "polyline": "", "summary_polyline": "" },
              "photos": { "primary": { "id": null, "unique_id": "", "urls": { "100": "", "600": "" } } },
              "highlighted_kudosers": [],
              "segment_efforts": [],
              "splits_metric": [],
              "laps": [],
              "gear": { "id": "g1", "name": "Shoes" }
            }
            """;

        using var httpClient = new HttpClient(new SequenceHttpMessageHandler(
            CreateOkResponse(new ThrowOnFirstReadContent(successJson)),
            CreateOkResponse(successJson)))
        {
            BaseAddress = new Uri("https://www.strava.com/api/v3/")
        };
        var api = new ActivitiesApi(httpClient);

        var activity = await api.GetActivity("token", "123");

        Assert.NotNull(activity);
        Assert.Equal(123, activity!.Id);
    }

    [Fact]
    public async Task GetActivitiesByAthlete_RetriesOnceWhenResponseBodyReadFails()
    {
        var successJson = """
            [
              {
                "athlete": { "id": 42 },
                "name": "Morning Run",
                "type": "Run",
                "sport_type": "Run",
                "id": 123,
                "start_date": "2024-01-01T00:00:00Z",
                "start_date_local": "2024-01-01T01:00:00Z",
                "timezone": "(GMT+01:00) Europe/Stockholm",
                "location_country": "Sweden",
                "map": { "id": "m1", "polyline": "", "summary_polyline": "" },
                "visibility": "everyone"
              }
            ]
            """;

        using var httpClient = new HttpClient(new SequenceHttpMessageHandler(
            CreateOkResponse(new ThrowOnFirstReadContent(successJson)),
            CreateOkResponse(successJson)))
        {
            BaseAddress = new Uri("https://www.strava.com/api/v3/")
        };
        var api = new ActivitiesApi(httpClient);

        var (activities, hasMorePages) = await api.GetActivitiesByAthlete("token");

        var activity = Assert.Single(activities!);
        Assert.Equal(123, activity.Id);
        Assert.False(hasMorePages);
    }

    [Fact]
    public async Task GetActivitiesByAthlete_RetriesMultipleTimesWhenResponseBodyReadFailsRepeatedly()
    {
        var successJson = """
            [
              {
                "athlete": { "id": 42 },
                "name": "Morning Run",
                "type": "Run",
                "sport_type": "Run",
                "id": 123,
                "start_date": "2024-01-01T00:00:00Z",
                "start_date_local": "2024-01-01T01:00:00Z",
                "timezone": "(GMT+01:00) Europe/Stockholm",
                "location_country": "Sweden",
                "map": { "id": "m1", "polyline": "", "summary_polyline": "" },
                "visibility": "everyone"
              }
            ]
            """;

        using var handler = new SequenceHttpMessageHandler(
            CreateOkResponse(new ThrowOnFirstReadContent(successJson)),
            CreateOkResponse(new ThrowOnFirstReadContent(successJson)),
            CreateOkResponse(new ThrowOnFirstReadContent(successJson)),
            CreateOkResponse(successJson));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://www.strava.com/api/v3/")
        };
        var api = new ActivitiesApi(httpClient);

        var (activities, hasMorePages) = await api.GetActivitiesByAthlete("token");

        var activity = Assert.Single(activities!);
        Assert.Equal(123, activity.Id);
        Assert.False(hasMorePages);
        Assert.Equal(4, handler.SendCount);
    }

    [Fact]
    public async Task GetActivity_DoesNotRetryRateLimitResponses()
    {
        using var handler = new SequenceHttpMessageHandler(
            CreateResponse(HttpStatusCode.TooManyRequests, "rate limited", headers =>
            {
                headers.Add("X-ReadRateLimit-Limit", "100,1000");
                headers.Add("X-ReadRateLimit-Usage", "100,500");
            }));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://www.strava.com/api/v3/")
        };
        var api = new ActivitiesApi(httpClient);

        await Assert.ThrowsAsync<StravaRateLimitExceededException>(() => api.GetActivity("token", "123"));

        Assert.Equal(1, handler.SendCount);
    }

    [Fact]
    public async Task GetActivity_RetriesOnceWhenSslHandshakeFails()
    {
        using var handler = new SequenceHttpMessageHandler(
            new HttpRequestException(
                "The SSL connection could not be established, see inner exception.",
                new AuthenticationException("Authentication failed because the remote party closed the transport stream.")),
            CreateOkResponse("""
                {
                  "id": 123,
                  "athlete": { "id": 42 },
                  "name": "Morning Run",
                  "type": "Run",
                  "sport_type": "Run",
                  "start_date": "2024-01-01T00:00:00Z",
                  "start_date_local": "2024-01-01T01:00:00Z",
                  "timezone": "(GMT+01:00) Europe/Stockholm",
                  "map": { "id": "m1", "polyline": "", "summary_polyline": "" },
                  "photos": { "primary": { "id": null, "unique_id": "", "urls": { "100": "", "600": "" } } },
                  "highlighted_kudosers": [],
                  "segment_efforts": [],
                  "splits_metric": [],
                  "laps": [],
                  "gear": { "id": "g1", "name": "Shoes" }
                }
                """));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://www.strava.com/api/v3/")
        };
        var api = new ActivitiesApi(httpClient);

        var activity = await api.GetActivity("token", "123");

        Assert.NotNull(activity);
        Assert.Equal(123, activity!.Id);
        Assert.Equal(2, handler.SendCount);
    }

    [Fact]
    public async Task GetActivitiesByAthlete_RetriesWhenHttpRequestExceptionHasOnlyTransientMessage()
    {
        using var handler = new SequenceHttpMessageHandler(
            new HttpRequestException("Error while copying content to a stream."),
            CreateOkResponse("""
                [
                  {
                    "athlete": { "id": 42 },
                    "name": "Morning Run",
                    "type": "Run",
                    "sport_type": "Run",
                    "id": 123,
                    "start_date": "2024-01-01T00:00:00Z",
                    "start_date_local": "2024-01-01T01:00:00Z",
                    "timezone": "(GMT+01:00) Europe/Stockholm",
                    "location_country": "Sweden",
                    "map": { "id": "m1", "polyline": "", "summary_polyline": "" },
                    "visibility": "everyone"
                  }
                ]
                """));
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("https://www.strava.com/api/v3/")
        };
        var api = new ActivitiesApi(httpClient);

        var (activities, hasMorePages) = await api.GetActivitiesByAthlete("token");

        var activity = Assert.Single(activities!);
        Assert.Equal(123, activity.Id);
        Assert.False(hasMorePages);
        Assert.Equal(2, handler.SendCount);
    }

    private static HttpResponseMessage CreateOkResponse(string content)
        => CreateResponse(HttpStatusCode.OK, content);

    private static HttpResponseMessage CreateOkResponse(HttpContent content)
        => new(HttpStatusCode.OK) { Content = content };

    private static HttpResponseMessage CreateResponse(HttpStatusCode statusCode, string content, Action<HttpResponseHeaders>? configureHeaders = null)
    {
        var response = new HttpResponseMessage(statusCode)
        {
            Content = new StringContent(content, Encoding.UTF8, "application/json")
        };
        configureHeaders?.Invoke(response.Headers);
        return response;
    }

    private sealed class SequenceHttpMessageHandler(params object[] responses) : HttpMessageHandler
    {
        private readonly Queue<object> _responses = new(responses);
        public int SendCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            SendCount++;
            if (_responses.Count == 0)
                throw new InvalidOperationException("No more responses configured.");

            var next = _responses.Dequeue();
            return next switch
            {
                HttpResponseMessage response => Task.FromResult(response),
                Exception exception => Task.FromException<HttpResponseMessage>(exception),
                _ => throw new InvalidOperationException($"Unsupported response type {next.GetType().FullName}.")
            };
        }
    }

    private sealed class ThrowOnFirstReadContent(string content) : HttpContent
    {
        private readonly byte[] _bytes = Encoding.UTF8.GetBytes(content);
        private int _copyCount;

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            if (Interlocked.Increment(ref _copyCount) == 1)
            {
                throw new HttpRequestException(
                    "Error while copying content to a stream.",
                    new IOException("The response ended prematurely."));
            }

            return stream.WriteAsync(_bytes, 0, _bytes.Length);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = _bytes.Length;
            return true;
        }
    }
}
